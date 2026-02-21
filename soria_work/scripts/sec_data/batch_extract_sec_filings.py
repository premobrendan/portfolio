"""
Batch extract financial data from SEC 8-K filings across multiple quarters and years.

This script:
1. Finds all 8-K Item 2.02 filings for specified ticker(s), quarters, and years
2. Downloads PDFs to temporary storage
3. Uses Gemini Flash for structure analysis
4. Creates smart chunks that respect table/section boundaries
5. Uses Gemini Pro to extract financial data
6. Processes multiple filings in parallel (configurable concurrency)
7. Cleans up temporary PDFs after extraction

Configure tickers, years, quarters, and concurrency at the bottom.
"""
import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF
import pandas as pd
import requests
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field
from sec_api import QueryApi

from soria_api.api_clients.llm_client import llm_client
from soria_api.integrations.llm_models import VertexAIModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# SEC API Key
SEC_API_KEY = "20065f397ad605187260b9de0e99c22e835f458e493dc14d1509f9e7558be60f"

# Model configuration
STRUCTURE_MODEL = VertexAIModel.GEMINI_FLASH  # Fast, cheap for structure analysis
EXTRACTION_MODEL = VertexAIModel.GEMINI_PRO_3  # Most accurate for data extraction
THINKING_BUDGET = 2048

# Chunking configuration
MAX_PAGES_PER_CHUNK = 8  # Maximum pages per chunk


# ============================================================================
# PYDANTIC SCHEMAS - STRUCTURE ANALYSIS
# ============================================================================

class DocumentSection(BaseModel):
    """A logical section of the document."""
    
    section_name: str = Field(
        description="Name of the section (e.g., 'Consolidated Statements of Income', 'Guidance', 'Membership Statistics')"
    )
    start_page: int = Field(
        description="Page number where this section starts (1-indexed)"
    )
    end_page: int = Field(
        description="Page number where this section ends (1-indexed)"
    )
    section_type: str = Field(
        description="Type of content: 'table', 'text', 'mixed'"
    )
    has_financial_data: bool = Field(
        description="Whether this section contains financial metrics to extract"
    )


class DocumentStructure(BaseModel):
    """Complete structure map of the document."""
    
    total_pages: int = Field(
        description="Total number of pages in the document"
    )
    sections: list[DocumentSection] = Field(
        description="List of all identified sections in order"
    )


# ============================================================================
# PYDANTIC SCHEMAS - DATA EXTRACTION
# ============================================================================

class FinancialMetric(BaseModel):
    """Schema for a single financial metric extraction."""
    
    metric_type: str = Field(
        description="Classify as 'Results' (historical) or 'Guidance' (forward-looking)"
    )
    period_described: str = Field(
        description="The specific period the data refers to (e.g., 'Q2 2022', 'FY 2022')"
    )
    table_section: str = Field(
        description="The specific header of the table or section title where the data was found"
    )
    metric: str = Field(
        description="Metric name with nested context concatenated using ' - '. For nested tables, combine parent and child headers (e.g., 'Revenue - OptumHealth' not just 'Revenue')"
    )
    original_value: str = Field(
        description="The raw text exactly as extracted (e.g., '$12.26', 'approx 425,000', '90.1% to 90.5%')"
    )
    cleaned_value: str = Field(
        description="Clean numbers (e.g., $110b → $110,000,000,000, 10% → 0.1)"
    )
    unit: str = Field(
        description="USD, Count (for membership/shares), or Ratio (for percentages)"
    )
    denomination: str = Field(
        default="",
        description="Denomination context like 'in millions', 'in billions', 'per share', or empty string if not applicable"
    )
    notes: str = Field(
        default="",
        description="Contextual qualifiers (e.g., 'GAAP', 'Non-GAAP', 'Approximate', 'At least')"
    )
    page_number: str = Field(
        default="",
        description="The page number where the data is located"
    )


class FinancialDataExtraction(BaseModel):
    """List of all extracted financial metrics."""
    
    rows: list[FinancialMetric] = Field(
        description="All financial metrics extracted from the document"
    )


# ============================================================================
# PROMPTS
# ============================================================================

STRUCTURE_ANALYSIS_PROMPT = """
Analyze this financial document and identify its logical structure.

Your task is to:
1. Identify all major sections (tables, text blocks, guidance sections, etc.)
2. Note the START and END page for each section
3. Determine if each section contains financial data worth extracting

Important rules:
- A "section" should be a complete, logical unit (e.g., one full table, one guidance block)
- DO NOT split tables across sections - if a table spans multiple pages, the section should include ALL those pages
- Sections can vary in length (1-10 pages)
- Prioritize sections with financial metrics (numbers, percentages, dollar amounts)
- For text-only pages (disclaimers, narratives), you can group multiple pages into one section

Output all identified sections in the order they appear.
"""

EXTRACTION_PROMPT = """
Task: Extract financial data from the provided earnings release PDF into a structured table.

FIRST: Identify the primary quarter and year this document covers by looking at the title, header, and main financial tables. 
This will be your reference for consistent period naming throughout.
Use this primary period as your baseline for calculating relative temporal references in the notes field.
For example, if the document is Q2 2023, use that as "current quarter" and calculate all other periods relative to it.

1. Extraction Scope:

For Actual Results (Historical): Scan all financial tables (Income Statement, Balance Sheet, Cash Flow, Segment Results, Membership, etc.). Extract ALL rows, but ONLY data from the column representing the Current Quarter (e.g., "Three Months Ended [Current Date]").

Strict Rule: Ignore Prior Year columns, Year-to-Date (YTD) columns, sequential comparisons, or percentage change columns.

For Guidance (Forward-Looking): Extract ALL forward-looking financial projections found in both text summaries and specific Guidance tables.

IMPORTANT - Avoid Derivative Metrics:
When absolute numbers are available (e.g., "Total Members Served", "Total Revenue"), DO NOT extract derivative/calculated metrics like:
- Growth rates (e.g., "Growth in Members Served")
- Percentage changes (e.g., "Year-over-Year Change")
- Differences or deltas
These can be calculated from the absolute values. Only extract if the absolute value is NOT present in the table.

2. Metric Type Classification:

CRITICAL: For each metric, determine if it is:
- "Results": Actual/historical data from current or past periods
- "Guidance": Forward-looking projections, outlook, or forecasted values

3. Metric Name Construction (VERY IMPORTANT):

For NESTED TABLES (tables with parent headers and row labels):
- Concatenate the parent header with the row label using " - "
- Example: If table has "Revenue" as a column header and "OptumHealth" as a row label → "Revenue - OptumHealth"
- Example: If table has "Earnings" as a column header and "UnitedHealthcare" as a row label → "Earnings - UnitedHealthcare"

CRITICAL - Consistency Within Tables:
- If you use concatenation for a table, apply it to ALL rows in that table, INCLUDING TOTALS
- Example: If using "People Served - Commercial", "People Served - Medicare", then ALSO use "People Served - Total Commercial" NOT just "Total Commercial"
- Example: If using "Revenue - Optum", "Revenue - UnitedHealthcare", then ALSO use "Revenue - Total" NOT just "Total"
- This ensures every metric is unambiguous and contextually clear

For SIMPLE TABLES (single-level row labels):
- Use the row label as-is
- Example: "Net revenues", "Operating income", "Total assets"

DO NOT include period descriptors (like "Full Year", "Outlook", "Q2") or denominations (like "in millions") in the metric name.

Metric Name Cleanup Rules:
- CRITICAL - Remove ALL quotation marks: Strip any quote characters (") from metric names. Example: "Revenue" → Revenue, "Operating Income" → Operating Income
- Remove footnote notifiers: "Net Earnings per Share - Adjusted (a)" → "Net Earnings per Share - Adjusted"
- Convert parenthetical descriptors to hyphens: "Medical Cost Ratio (Standardized)" → "Medical Cost Ratio - Standardized"
- Standardize income/loss patterns: "Income (Loss)", "(Loss) Income", "Loss (Income)" → "Income/Loss"
- Standardize gain/loss patterns: "Gain (Loss)", "(Loss) Gain", "Loss (Gain)" → "Gain/Loss"
- Remove empty parentheses: "Revenue ()" → "Revenue"
- CRITICAL - Make metrics self-descriptive: Metrics that are only "Adjusted (non-GAAP)" or "GAAP" are NOT descriptive enough. 
  Add context about WHAT is being measured. 
  Examples: 
  * "Adjusted (non-GAAP)" → "Adjusted Earnings per Share" or "Adjusted Operating Income"
  * "GAAP" → "GAAP Net Income" or "GAAP Operating Margin"
  * Always include WHAT financial measure is being reported, not just the accounting method
- Keep metric names clean and consistent

Examples of standardization:
- "Net Investment (Gains) Losses" → "Net Investment Gains/Losses"
- "Pre-Tax Adjusted Income (Loss) From Operations" → "Pre-Tax Adjusted Income/Loss From Operations"
- "Shareholders' (Loss) Income" → "Shareholders' Income/Loss"

4. Denomination Extraction:

CRITICAL: Look for denomination indicators in table headers, footnotes, or column headers:
- Extract phrases like: "in millions", "in billions", "per share", "in thousands"
- Store in the separate "denomination" column
- If no denomination is specified, leave empty

DO NOT include time periods like "Full Year" or "Outlook" in denomination.

5. Data Formatting Rules:

Cleaned Values: Convert words (e.g., "billion", "million") into full numeric integers (e.g., 1,000,000,000).

Percentages: Convert to decimals (e.g., 90.5% → 0.905).

CRITICAL - Handling Ranges (High/Low):
When a chart describes a HIGH and LOW end (e.g., separate columns for "Low" and "High", or "Range Low" and "Range High"), these should be treated as RANGES:
- Extract as a range in original_value using "X to Y" format (e.g., "90.1% to 90.5%" or "$100M to $110M")
- In cleaned_value, use the same "X to Y" format with cleaned numbers (e.g., "0.901 to 0.905" or "100000000 to 110000000")
- Do NOT extract the low and high as separate metrics - combine them into a single range metric
- Example: If table has columns "Revenue Low" = "$100M" and "Revenue High" = "$110M", extract as:
  * metric: "Revenue"
  * original_value: "$100M to $110M"
  * cleaned_value: "100000000 to 110000000"
  * notes: "Projected revenue range for [period] representing low to high end of guidance"

Negatives/Declines: Represent net losses or membership declines as negative numbers.

CRITICAL - Sign Handling:
- Values in parentheses are NEGATIVE: "(178)" → -178
- Loss context means NEGATIVE: "Net loss of $50M" → -50000000
- Gain context means POSITIVE: "Net gain of $50M" → 50000000
- For metrics like "Income/Loss", check if the actual value is income (positive) or loss (negative)
- For metrics like "Gains/Losses", check if the actual value is a gain (positive) or loss (negative)
- Ensure the sign in cleaned_value matches the actual meaning of the original_value

Qualitative: If a value is described as "flat" or "relatively flat", input 0 in the cleaned value column.

6. Output Table Schema:

Create a table with the following columns:

Metric Type: Either "Results" or "Guidance"

Period Described: The period the data refers to.
CRITICAL FORMAT REQUIREMENT - Use ONLY these formats:
- For Results (current quarter data): "Q1 YYYY", "Q2 YYYY", "Q3 YYYY", or "Q4 YYYY" where YYYY is the year
- For Guidance (forward-looking data): "FY YYYY" where YYYY is the target year
CRITICAL: Use consistent format across the entire PDF. DO NOT use formats like "3Q25", "2Q 2025", etc.
Examples: "Q2 2022" for second quarter 2022 results, "FY 2022" for full year 2022 guidance.
Maintain consistency - if you start with "Q2 2022", use that format for all Q2 2022 metrics.

Table/Section: The specific header of the table or section title where the data was found. 
CRITICAL: Use proper case (Title Case), not ALL CAPS. Preserve proper company name capitalization.
CRITICAL: Remove ALL specific years (e.g., "2025", "2024", "2023", "2022", "2021", "2020", "2019") entirely from table/section names.
CRITICAL: Remove "(Unaudited)" from table names as it is not descriptive.
CRITICAL: Remove year references in ALL positions - beginning, middle, or end of the table name.
CRITICAL: Replace ordinal quarter references with "Quarterly" (e.g., "First Quarter" → "Quarterly", "Second Quarter" → "Quarterly", "Third Quarter" → "Quarterly", "Fourth Quarter" → "Quarterly").

Examples of year removal:
- "2023 Revised Guidance" → "Revised Guidance"
- "FY 2022 Guidance" → "FY Guidance" 
- "UnitedHealth Group Reports Second Quarter 2021 Results" → "UnitedHealth Group Reports Quarterly Results"
- "Humana Reports Second Quarter 2022 Financial Results" → "Humana Reports Quarterly Financial Results"
- "Supplemental Non-GAAP Disclosures 2023 Operating Results Forecast" → "Supplemental Non-GAAP Disclosures Operating Results Forecast"
- "FY 2022 Projected Adjusted Results Exclusions" → "FY Projected Adjusted Results Exclusions"

Examples of quarter ordinal replacement:
- "First Quarter Results" → "Quarterly Results"
- "Second Quarter Financial Highlights" → "Quarterly Financial Highlights"
- "Third Quarter Earnings Report" → "Quarterly Earnings Report"
- "Fourth Quarter Performance Summary" → "Quarterly Performance Summary"

Examples of other formatting:
- "Consolidated Statements of Income" NOT "CONSOLIDATED STATEMENTS OF INCOME"
- "Supplemental Financial Information - Businesses" NOT "SUPPLEMENTAL FINANCIAL INFORMATION - BUSINESSES"  
- "UnitedHealth Group Outlook" NOT "UnitedHealth Group 2025 Outlook"
- "Earnings Guidance" NOT "2025 Earnings Guidance"
- "Financial Highlights" NOT "Financial Highlights (Unaudited)"
- "Consolidated Income Statements" NOT "Consolidated Income Statements (Unaudited)"

Metric: Constructed metric name with nested context (e.g., "Revenue - OptumHealth", "Operating margin - UnitedHealthcare"). 
CRITICAL: Remove ALL quotation marks (") from the metric name. The metric should never contain quote characters.

Original Value: The raw text exactly as extracted (e.g., "$12.26", "approx 425,000", "90.1% to 90.5%").

Cleaned Value: Clean numbers so that $110b is shown as $110,000,000,000 or 10% is 0.1 etc.

Unit: USD, Count (for membership/shares), or Ratio (for percentages).

Denomination: Scale indicators like "in millions", "in billions", "per share" (extracted from table headers/footnotes).

Notes: CRITICAL - Provide comprehensive, descriptive commentary about what this metric represents. 
The goal is to make the metric fully understandable without reading the source document.
Be as descriptive as possible by synthesizing information from multiple sources.

CRITICAL - Build Comprehensive Context:
Combine information from:
1. The metric name itself - what it says
2. The table/section name - what context the table provides
3. Surrounding context - row/column headers, table footnotes, explanatory text
4. Missing context - information not obvious from the metric name alone

Examples of synthesizing context:
- If metric is "Revenues" from table "Key Second Quarter Metrics" → "Total consolidated revenues for the current quarter"
- If metric is "Operating Margin - Optum" from table "Supplemental Financial Information" → "Operating margin percentage for Optum segment for the current quarter"
- If metric is "Medical Cost Ratio" with surrounding note about "standardized" → "Standardized medical cost ratio excluding COVID-19 impacts for the current quarter"

CRITICAL - Dynamic Temporal References:
Calculate relative time references based on the document's period:

For the CURRENT period being reported:
- Use "current quarter" for the main quarter being reported
- Use "current year" for the year being reported
- Use "full year" for full year guidance/projections

For PAST periods (calculate the time difference):
- One quarter ago: "prior quarter" or "previous quarter"
- Two quarters ago: "two quarters ago"
- Three quarters ago: "three quarters ago"
- Same quarter last year: "prior year same quarter" or "year-over-year"
- One year ago: "prior year"
- Two years ago: "two years ago"
- Full prior year: "prior year" or "prior fiscal year"

For FUTURE periods:
- Next quarter: "next quarter"
- Full year ahead: "full year" or "projected full year"
- Two years ahead: "two years forward"

Examples:
- If document is Q2 2023 and metric refers to Q2 2022 → "prior year same quarter"
- If document is Q2 2023 and metric refers to Q1 2023 → "prior quarter"
- If document is Q2 2023 and metric refers to FY 2022 → "prior year"
- If document is Q2 2023 and metric refers to Q4 2022 → "two quarters ago"
- If document is Q2 2023 and metric refers to FY 2023 → "full year"

Keep generic relative terms as they are: "year-over-year", "quarter end", "sequential", "period end"

CRITICAL - Information to Include:
- Accounting method: "GAAP", "Non-GAAP", "Adjusted" with explanation of what's excluded/adjusted
- Adjustment details: If metric is adjusted, specify what's being excluded/adjusted (e.g., "excluding 1x transaction costs", "adjusted for special items", "excluding COVID-19 impacts", "excluding one-time charges")
- Business context: Which segment, product line, service area, or geography
- Segment/Company scope: ALWAYS specify if the metric is for "consolidated operations", a specific segment (e.g., "Optum segment", "UnitedHealthcare segment"), or a business unit
- Calculation method: "Per member per month", "As a percentage of revenue", "Year-over-year change"
- Scope: "Consolidated", "Segment-specific", "Product-specific"
- Timing context: "As of quarter end", "Average for the period", "Projected for full year"
- Special qualifiers: "Excluding special items", "Before tax", "After tax", "Standardized"
- Source of metric: What financial statement or schedule it comes from

Examples of EXCELLENT notes (very descriptive with dynamic temporal references):
- "GAAP diluted earnings per share for consolidated operations for the current quarter"
- "Non-GAAP adjusted EBITDA excluding acquisition-related costs and restructuring charges for consolidated operations for the current quarter"
- "Adjusted operating margin for Optum segment excluding 1x transaction costs for the current quarter"
- "Total medical membership count for Medicare Advantage product line at end of current quarter"  
- "GAAP operating margin percentage for Retail segment excluding special items for current quarter"
- "Non-GAAP adjusted earnings per share for consolidated operations excluding COVID-19 testing impacts for the current quarter"
- "Projected full year total revenues guidance for consolidated operations representing low end of range"
- "Average monthly premium revenue per member for commercial fully-insured products for UnitedHealthcare segment for the current quarter"
- "Year-over-year percentage increase in same facility admissions comparing current quarter to prior year same quarter"
- "Cash flows from operating activities for consolidated operations for the current quarter"
- "Total revenues for OptumHealth segment for prior quarter"
- "Operating income for consolidated operations for prior year same quarter"
- "Adjusted operating income for UnitedHealthcare segment excluding one-time regulatory charges for current quarter"
- "Acute care hospital admissions on a same-facility basis for two quarters ago"
- "Full year projected earnings per share for consolidated operations for current year"
- "Medical cost ratio for consolidated operations for prior year"

Examples of GOOD notes (descriptive):
- "Non-GAAP EPS excluding amortization for the current quarter"
- "Total revenues for the current quarter"

Examples of BAD notes (too generic - not acceptable):
- "Segment metric" ❌
- "Revenue" ❌
- "GAAP" ❌
- "Current quarter" ❌

Examples of BAD notes (specific dates instead of dynamic relative references):
- "Q2 2023 operating income" ❌ (should be "Operating income for the current quarter")
- "Q2 2022 operating income" ❌ (should be "Operating income for prior year same quarter")
- "Q1 2023 revenues" ❌ (should be "Revenues for prior quarter")
- "2023 full year guidance" ❌ (should be "Full year guidance" or "Full year projected guidance")
- "FY 2022 earnings" ❌ (should be "Earnings for prior year")

Make every note as informative and contextual as possible with generic temporal references.

Page Number: The ACTUAL page number where the data is located in the PDF. Look carefully at page numbers visible in the document (often in headers or footers). Report the page number as it appears in the document, not the PDF page index.

CRITICAL REMINDERS:
1. REMOVE ALL QUOTATION MARKS (") from metric names - metrics should never contain quote characters
2. For nested tables: Concatenate parent header + " - " + row label
3. Apply concatenation consistently to ALL rows including totals (e.g., "Revenue - Total" not just "Total")
4. Extract denominations to separate column, don't put in metric name
5. Classify each metric as "Results" or "Guidance"
6. Verify page numbers from document headers/footers
7. Skip derivative metrics (growth rates, % changes) when absolute values are present
8. Remove footnote notifiers from metrics: "(a)", "(b)", "(c)" etc.
9. Convert parenthetical descriptors to hyphens: "(Standardized)" → "- Standardized"
10. Standardize income/loss patterns: "Income (Loss)" → "Income/Loss", "(Loss) Income" → "Income/Loss"
11. Standardize gain/loss patterns: "Gain (Loss)" → "Gain/Loss", "(Loss) Gain" → "Gain/Loss"
12. Use proper case for table/section names (not ALL CAPS)
13. Remove "(Unaudited)" from table/section names
14. Remove ALL specific years entirely from table/section names in ALL positions (e.g., "2025 Outlook" → "Outlook", "FY 2022 Guidance" → "FY Guidance", "Reports Q2 2023 Results" → "Reports Q2 Results")
14a. Replace ordinal quarter references with "Quarterly" (e.g., "First Quarter Results" → "Quarterly Results", "Second Quarter" → "Quarterly")
15. Use consistent period format: "Q1 YYYY", "Q2 YYYY", etc. for Results or "FY YYYY" for Guidance
16. Ensure cleaned_value signs are correct: parentheses = negative, losses = negative, gains = positive
17. Make metrics self-descriptive: "Adjusted" or "GAAP" alone isn't enough - specify WHAT is being measured
18. Provide detailed, meaningful commentary in Notes field including accounting method, business context, calculation method, timing, and qualifiers
19. In Notes field, use dynamic temporal references: calculate relative time (e.g., "prior quarter", "prior year same quarter", "two quarters ago") instead of specific dates
"""


# ============================================================================
# FILING METADATA
# ============================================================================

class FilingMetadata(BaseModel):
    """Metadata for a single filing."""
    ticker: str
    quarter: int
    year: int
    filed_date: str
    exhibit_type: str
    url: str


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def split_pdf_pages(pdf_path: str, pages: list[int], output_path: str) -> str:
    """Extract specific pages from a PDF and save to a new file."""
    doc = fitz.open(pdf_path)
    new_doc = fitz.open()
    
    for page_num in pages:
        if 0 <= page_num < len(doc):
            new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
        else:
            logger.warning(f"Page {page_num} out of range for {pdf_path}")
    
    new_doc.save(output_path)
    new_doc.close()
    doc.close()
    
    return output_path


def get_pdf_page_count(pdf_path: str) -> int:
    """Get the total number of pages in a PDF."""
    doc = fitz.open(pdf_path)
    page_count = len(doc)
    doc.close()
    return page_count


def create_smart_chunks(sections: list[DocumentSection], max_pages: int) -> list[dict]:
    """Create chunks based on document structure, respecting section boundaries."""
    chunks = []
    current_chunk_sections = []
    current_start = None
    current_end = None
    current_has_data = False
    
    for section in sections:
        section_pages = section.end_page - section.start_page + 1
        
        # If this section alone exceeds max_pages, make it its own chunk
        if section_pages > max_pages:
            # Flush current chunk if any
            if current_chunk_sections:
                chunks.append({
                    'start_page': current_start,
                    'end_page': current_end,
                    'sections': current_chunk_sections,
                    'has_data': current_has_data
                })
                current_chunk_sections = []
                current_has_data = False
            
            # Add oversized section as its own chunk
            chunks.append({
                'start_page': section.start_page,
                'end_page': section.end_page,
                'sections': [section.section_name],
                'has_data': section.has_financial_data
            })
            current_start = None
            current_end = None
            continue
        
        # Calculate what the chunk size would be if we add this section
        if current_start is None:
            potential_pages = section_pages
        else:
            potential_pages = section.end_page - current_start + 1
        
        # If adding this section would exceed max_pages, start a new chunk
        if potential_pages > max_pages and current_chunk_sections:
            chunks.append({
                'start_page': current_start,
                'end_page': current_end,
                'sections': current_chunk_sections,
                'has_data': current_has_data
            })
            current_chunk_sections = []
            current_has_data = False
            current_start = None
            current_end = None
        
        # Add section to current chunk
        if current_start is None:
            current_start = section.start_page
        current_end = section.end_page
        current_chunk_sections.append(section.section_name)
        current_has_data = current_has_data or section.has_financial_data
    
    # Flush final chunk
    if current_chunk_sections:
        chunks.append({
            'start_page': current_start,
            'end_page': current_end,
            'sections': current_chunk_sections,
            'has_data': current_has_data
        })
    
    return chunks


# ============================================================================
# FILING DISCOVERY
# ============================================================================

def find_filing(ticker: str, quarter: int, year: int) -> Optional[FilingMetadata]:
    """
    Find 8-K Item 2.02 filing with Exhibit 99.2 or 99.1.
    
    Returns:
        FilingMetadata or None
    """
    query_api = QueryApi(api_key=SEC_API_KEY)
    
    # Calculate date range based on quarter
    quarter_end_months = {1: (3, 4), 2: (6, 7), 3: (9, 10), 4: (12, 1)}
    start_month, end_month = quarter_end_months[quarter]
    end_year = year + 1 if quarter == 4 else year
    
    search_query = f'ticker:{ticker} AND formType:"8-K" AND "Item 2.02" AND filedAt:[{year}-{start_month:02d}-01 TO {end_year}-{end_month:02d}-30]'
    
    search_params = {
        "query": search_query,
        "from": "0",
        "size": "10",
        "sort": [{"filedAt": {"order": "desc"}}],
    }
    
    logger.info(f"Searching for {ticker} Q{quarter} {year}...")
    
    try:
        response = query_api.get_filings(search_params)
    except Exception as e:
        logger.error(f"API error for {ticker} Q{quarter} {year}: {e}")
        return None
    
    if not response.get('filings'):
        logger.warning(f"No filings found for {ticker} Q{quarter} {year}")
        return None
    
    # Try to find Exhibit 99.2 first, then 99.1
    for filing in response['filings']:
        filed_date = filing['filedAt']
        
        # First pass: look for Exhibit 99.2
        for file in filing['documentFormatFiles']:
            file_type = file.get('type', '').lower()
            if '99.2' in file_type or 'ex99-2' in file_type or 'ex992' in file_type:
                url = file['documentUrl']
                logger.info(f"  → Found {ticker} Q{quarter} {year} Exhibit 99.2")
                return FilingMetadata(
                    ticker=ticker,
                    quarter=quarter,
                    year=year,
                    filed_date=filed_date,
                    exhibit_type="99.2",
                    url=url
                )
        
        # Second pass: look for Exhibit 99.1
        for file in filing['documentFormatFiles']:
            file_type = file.get('type', '').lower()
            if '99.1' in file_type or 'ex99-1' in file_type or 'ex991' in file_type:
                url = file['documentUrl']
                logger.info(f"  → Found {ticker} Q{quarter} {year} Exhibit 99.1")
                return FilingMetadata(
                    ticker=ticker,
                    quarter=quarter,
                    year=year,
                    filed_date=filed_date,
                    exhibit_type="99.1",
                    url=url
                )
    
    logger.warning(f"No suitable exhibit found for {ticker} Q{quarter} {year}")
    return None


def find_all_filings(ticker: str, years: list[int], quarters: list[int]) -> list[FilingMetadata]:
    """Find all filings for specified years and quarters."""
    filings = []
    
    for year in years:
        for quarter in quarters:
            filing = find_filing(ticker, quarter, year)
            if filing:
                filings.append(filing)
            # Small delay to avoid API rate limits
            time.sleep(0.2)
    
    return filings


# ============================================================================
# PDF DOWNLOAD
# ============================================================================

async def download_pdf_from_url(url: str, output_path: str):
    """Download PDF from URL using Playwright."""
    headers = {
        "User-Agent": "Cameron Spiller cameron@soriaresearch.com",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov"
    }
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context()
        await context.set_extra_http_headers(headers)
        page = await context.new_page()
        
        try:
            url_lower = url.lower()
            
            # For HTML/text files, fetch content first then load locally
            if url_lower.endswith(('.txt', '.htm', '.html')):
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    temp_dir = Path(__file__).parent / 'temp'
                    temp_dir.mkdir(exist_ok=True)
                    # Use unique filename with timestamp and random component
                    import uuid
                    temp_filename = f"temp_{int(time.time())}_{uuid.uuid4().hex[:8]}.html"
                    temp_html_path = temp_dir / temp_filename
                    
                    if url_lower.endswith('.txt'):
                        html_content = f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>SEC Document</title>
                            <style>
                                body {{
                                    font-family: monospace;
                                    white-space: pre-wrap;
                                    margin: 20px;
                                }}
                            </style>
                        </head>
                        <body>
                            <pre>{response.text}</pre>
                        </body>
                        </html>
                        """
                        temp_html_path.write_text(html_content, encoding='utf-8')
                    else:
                        temp_html_path.write_text(response.text, encoding='utf-8')
                    
                    file_url = f"file://{temp_html_path.absolute()}"
                    await page.goto(file_url, wait_until='networkidle', timeout=60000)
                    temp_html_path.unlink()
                else:
                    logger.error(f"Failed to fetch content: {response.status_code}")
                    return False
            else:
                await page.goto(url, wait_until='networkidle', timeout=60000)
            
            await page.wait_for_timeout(2000)
            await page.pdf(path=output_path, format="Letter")
            
            return True
            
        except Exception as e:
            logger.error(f"Error downloading PDF: {e}")
            return False
        finally:
            await browser.close()


# ============================================================================
# STRUCTURE ANALYSIS
# ============================================================================

def analyze_document_structure(pdf_path: str) -> Optional[DocumentStructure]:
    """Use Gemini Flash to analyze the document structure."""
    try:
        file_ref = llm_client.upload_file(pdf_path)
        structure_config = {"response_schema": DocumentStructure}
        
        response = llm_client.generate_content(
            model=STRUCTURE_MODEL,
            contents=[file_ref, STRUCTURE_ANALYSIS_PROMPT],
            config=structure_config,
        )
        
        if isinstance(response, dict):
            response = DocumentStructure(**response)
        elif not isinstance(response, DocumentStructure):
            logger.error(f"Unexpected response type: {type(response)}")
            return None
        
        return response
    except Exception as e:
        logger.error(f"Structure analysis failed: {e}")
        return None


# ============================================================================
# DATA EXTRACTION
# ============================================================================

def extract_chunk_to_df(chunk_pdf_path: str, prompt: str, chunk_info: dict, ticker: str = None, quarter: int = None, year: int = None) -> pd.DataFrame:
    """Extract data from a PDF chunk using Gemini Pro."""
    if not chunk_info['has_data']:
        return pd.DataFrame()
    
    try:
        file_ref = llm_client.upload_file(chunk_pdf_path)
        
        # Add page range context
        page_context = f"\n\nCONTEXT: This chunk contains pages {chunk_info['start_page']}-{chunk_info['end_page']} of the original document. When reporting page numbers, verify they fall within this range or use the visible page numbers in the document headers/footers."
        
        # Add target period context for consistency
        if ticker and quarter and year:
            target_quarter_year = f"Q{quarter} {year}"
            page_context += f"\n\nTARGET PERIOD: This is a {ticker} {target_quarter_year} filing. Ensure all period references use consistent naming (e.g., '{target_quarter_year}' not '3Q{year}' or other variations)."
        
        prompt_with_context = prompt + page_context
        
        extraction_config = {"response_schema": FinancialDataExtraction}
        if THINKING_BUDGET:
            extraction_config["thinking_budget"] = THINKING_BUDGET
        
        response = llm_client.generate_content(
            model=EXTRACTION_MODEL,
            contents=[file_ref, prompt_with_context],
            config=extraction_config,
        )
        
        if response:
            if isinstance(response, dict) and 'rows' in response:
                return pd.DataFrame(response['rows'])
            elif isinstance(response, FinancialDataExtraction):
                rows = [row.model_dump() for row in response.rows]
                return pd.DataFrame(rows)
        
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Extraction failed for chunk: {e}")
        return pd.DataFrame()


def extract_pdf_with_smart_chunking(pdf_path: str, ticker: str = None, quarter: int = None, year: int = None, temp_dir: Path = None) -> pd.DataFrame:
    """Extract data from a PDF using intelligent chunking."""
    # Phase 1: Analyze structure
    doc_structure = analyze_document_structure(pdf_path)
    if not doc_structure:
        logger.error("Structure analysis failed")
        return pd.DataFrame()
    
    # Phase 2: Create smart chunks
    chunks = create_smart_chunks(doc_structure.sections, MAX_PAGES_PER_CHUNK)
    
    # Phase 3: Extract data from chunks
    # Use provided temp_dir or create default
    if temp_dir is None:
        temp_dir = Path(__file__).parent / 'temp'
        temp_dir.mkdir(exist_ok=True)
    
    all_dfs = []
    
    for idx, chunk in enumerate(chunks, 1):
        if not chunk['has_data']:
            continue
        
        # Create chunk PDF
        page_range = list(range(chunk['start_page'] - 1, chunk['end_page']))
        chunk_filename = f"chunk_{idx:03d}_pages_{chunk['start_page']}-{chunk['end_page']}.pdf"
        chunk_path = temp_dir / chunk_filename
        
        split_pdf_pages(pdf_path, page_range, str(chunk_path))
        
        # Extract data
        chunk_df = extract_chunk_to_df(str(chunk_path), EXTRACTION_PROMPT, chunk, ticker, quarter, year)
        
        if not chunk_df.empty:
            all_dfs.append(chunk_df)
        
        # Clean up chunk file
        chunk_path.unlink()
    
    # Combine all DataFrames
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()


# ============================================================================
# MAIN PROCESSING
# ============================================================================

async def process_filing(filing: FilingMetadata, output_dir: Path) -> Optional[Path]:
    """
    Process a single filing: download, extract, and save.
    
    Returns:
        Path to the CSV file or None if failed
    """
    ticker = filing.ticker
    quarter = filing.quarter
    year = filing.year
    exhibit = filing.exhibit_type.replace('.', '')
    
    logger.info(f"{'=' * 60}")
    logger.info(f"Processing {ticker} Q{quarter} {year} (Exhibit {filing.exhibit_type})")
    logger.info(f"{'=' * 60}")
    
    # Create a UNIQUE temp directory for this filing to avoid conflicts
    temp_dir = Path(tempfile.mkdtemp(prefix=f"{ticker}_Q{quarter}_{year}_"))
    
    # Generate temporary PDF path
    pdf_filename = f"{ticker}_Q{quarter}_{year}_{filing.filed_date[:10]}_ex{exhibit}.pdf"
    pdf_path = temp_dir / pdf_filename
    
    # Download PDF
    logger.info(f"  [1/3] Downloading PDF...")
    success = await download_pdf_from_url(filing.url, str(pdf_path))
    
    if not success or not pdf_path.exists():
        logger.error(f"  ❌ Failed to download PDF for {ticker} Q{quarter} {year}")
        return None
    
    try:
        # Extract data - run in thread to avoid blocking
        logger.info(f"  [2/3] Extracting data with smart chunking...")
        loop = asyncio.get_event_loop()
        
        # Create a unique chunk temp dir for this filing
        chunk_temp_dir = temp_dir / f"chunks_{ticker}_Q{quarter}_{year}"
        chunk_temp_dir.mkdir(exist_ok=True)
        
        df = await loop.run_in_executor(
            None,  # Use default executor
            extract_pdf_with_smart_chunking,
            str(pdf_path),
            ticker,
            quarter,
            year,
            chunk_temp_dir
        )
        
        if df.empty:
            logger.warning(f"  ⚠️  No data extracted for {ticker} Q{quarter} {year}")
            return None
        
        # Save to CSV
        logger.info(f"  [3/3] Saving extracted data...")
        csv_filename = f"{ticker}_Q{quarter}_{year}_{filing.filed_date[:10]}_ex{exhibit}_extracted.csv"
        csv_path = output_dir / csv_filename
        df.to_csv(csv_path, index=False)
        
        logger.info(f"  ✅ Successfully extracted {len(df)} metrics → {csv_filename}")
        
        return csv_path
        
    except Exception as e:
        logger.error(f"  ❌ Error processing {ticker} Q{quarter} {year}: {e}")
        return None
    finally:
        # Clean up entire temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info(f"  🗑️  Cleaned up temporary directory")


async def process_filings_batch(filings: list[FilingMetadata], output_dir: Path, concurrency: int = 4):
    """
    Process multiple filings with controlled concurrency.
    
    Args:
        filings: List of filing metadata
        output_dir: Directory to save extracted CSVs
        concurrency: Number of concurrent extractions
    """
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"\n{'=' * 80}")
    logger.info(f"BATCH EXTRACTION")
    logger.info(f"Total filings: {len(filings)}")
    logger.info(f"Concurrency: {concurrency}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"{'=' * 80}\n")
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_with_semaphore(filing):
        async with semaphore:
            return await process_filing(filing, output_dir)
    
    # Process all filings
    tasks = [process_with_semaphore(filing) for filing in filings]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Summary
    successful = sum(1 for r in results if r and not isinstance(r, Exception))
    failed = len(results) - successful
    
    logger.info(f"\n{'=' * 80}")
    logger.info(f"BATCH EXTRACTION COMPLETE")
    logger.info(f"Successful: {successful}/{len(filings)}")
    logger.info(f"Failed: {failed}/{len(filings)}")
    logger.info(f"{'=' * 80}\n")
    
    return results


def main():
    """Main function for batch extraction."""
    # Configuration
    TICKER = "THC"
    YEARS = [2023,2024] # list[int](range(2015, 2024))
    QUARTERS = [1,2,3,4]
    CONCURRENCY = 4  # Process 4 filings at a time
    
    logger.info(f"{'=' * 80}")
    logger.info(f"SEC BATCH FINANCIAL DATA EXTRACTION")
    logger.info(f"Ticker: {TICKER}")
    logger.info(f"Years: {', '.join(map(str, YEARS))}")
    logger.info(f"Quarters: {', '.join(map(str, QUARTERS))}")
    logger.info(f"{'=' * 80}\n")
    
    # Step 1: Find all filings
    logger.info("Step 1: Finding all filings...")
    filings = find_all_filings(TICKER, YEARS, QUARTERS)
    
    if not filings:
        logger.error("No filings found!")
        return
    
    logger.info(f"\n✅ Found {len(filings)} filings:")
    for filing in filings:
        logger.info(f"  - {filing.ticker} Q{filing.quarter} {filing.year} (Exhibit {filing.exhibit_type})")
    
    # Step 2: Process all filings in parallel
    logger.info(f"\nStep 2: Processing filings with concurrency={CONCURRENCY}...")
    
    output_dir = Path(__file__).parent / "sec_filings"
    
    results = asyncio.run(process_filings_batch(filings, output_dir, CONCURRENCY))
    
    # Display results
    logger.info("\n📊 Final Results:")
    for filing, result in zip(filings, results):
        if result and not isinstance(result, Exception):
            logger.info(f"  ✅ {filing.ticker} Q{filing.quarter} {filing.year}: {result.name}")
        else:
            logger.info(f"  ❌ {filing.ticker} Q{filing.quarter} {filing.year}: Failed")
    
    logger.info(f"\n✅ All done! Check {output_dir} for extracted data.")


if __name__ == "__main__":
    main()

