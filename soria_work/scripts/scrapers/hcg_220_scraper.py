import logging
import time
from io import StringIO

import pandas as pd
import requests
from sqlalchemy.orm import Session

from soria_api.diglet.base import BaseScraper
from soria_api.diglet.types import DownloadResult, SourceData
from soria_api.models import Source
from soria_api.models.source import APILoadStrategy, SourceType
from soria_api.models.source_file import FileType, SourceFile

logger = logging.getLogger(__name__)


class HCG220RateReview(BaseScraper):
    """Scraper for HCG Health Plan Information."""

    PAGE_URL = "https://ratereview.healthcare.gov/"
    API_LOAD_STRATEGY = APILoadStrategy.INCREMENTAL
    SOURCE_TYPE = SourceType.API
    IS_PARENT = False

    STATE_OPTIONS = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]

    YEARS_URL = "https://ratereview.healthcare.gov/ratereviewservices/urr/years"

    HEADERS = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "dnt": "1",
        "if-none-match": 'W/"32-aTGN4mRrL0DV3CI+wutUKwNfhE0"',
        "priority": "u=1, i",
        "referer": "https://ratereview.healthcare.gov/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    MARKET_TYPE = "Individual"

    SLEEP_TIMER = 0.2

    @property
    def source_key(self) -> str:
        return "HCG_220_rate_review_premiums"

    @property
    def source_name(self) -> str:
        return "hcg220 rate review premiums"

    @property
    def scraper_id(self):
        return "HCG_220"

    def fetch_raw_data(self) -> list[dict]:
        """
        Fetch all selections for information
        """
        try:
            response = requests.get(self.YEARS_URL, headers=self.HEADERS)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

            # If the response is JSON, you can parse it
            if "application/json" in response.headers.get("Content-Type", ""):
                year_options = response.json()

            # compile raw data dictionaries
            raw_data = []
            for year in year_options:
                raw_data.append(
                    {
                        "table_id": f"rate_review_data_{year}",
                        "year": year,
                        "file_url": self._build_data_url("NY", year),  # just a dummy url
                    }
                )

            logger.info(f"Found {len(raw_data)} potentials to scrape")
            return raw_data

        except Exception as e:
            logger.error(f"Error fetching raw data: {e}")
            return []

    def find_new_data(self, available: list[SourceData], db_session: Session) -> list[SourceData]:
        # Get source
        logger.info(f"Looking for source: {self.source_key}")
        source = db_session.query(Source).filter_by(name=self.source_key).first()
        if not source:
            logger.warning(f"Source {self.source_key} not found")
            return available

        # Get existing files
        existing_files = db_session.query(SourceFile).filter_by(source_id=source.id).all()
        existing_table_ids = {
            f.ingestion_metadata.get("table_id") for f in existing_files if f.ingestion_metadata
        }

        # Filter out already scraped data
        new_data = []
        for data in available:
            table_id = data.metadata.get("table_id")
            if table_id and table_id not in existing_table_ids:
                new_data.append(data)

        logger.info(f"Found {len(new_data)} new tables to scrape out of {len(available)} available")
        return new_data

    def download_files(self, data: SourceData) -> DownloadResult:
        """Download file content."""
        table_id = data.metadata.get("table_id")
        year = data.metadata.get("year")

        logger.info(f"Extracting rate review data for {year}")

        # get table info for each state of the year
        year_table_data = []
        year_products_data = []

        for state in self.STATE_OPTIONS:
            aquisition_dict = {"table_id": table_id, "data_url": self._build_data_url(state, year)}

            table_data = self._try_get_table(aquisition_dict)
            if not table_data:
                logger.debug(f"No table found with params {state} {year}")
                continue
            else:
                logger.info(f"Extracted table with params {state} {year}")
                # If table has info, find all product information
                products_data = self._try_get_products_info(table_data)
                if not products_data:
                    logger.debug("No product information found")

            year_table_data += table_data
            year_products_data += products_data

        if not year_table_data:
            logger.warning(f"No table data found for any state in year {year}")
            raise ValueError(f"No table data found for any state in year {year}")

        if not year_products_data:
            logger.warning(f"No product data found for any state in year {year}")
            raise ValueError(f"No product data found for any state in year {year}")

        # convert tables to DataFrame
        df_year_table_data = pd.DataFrame.from_dict(year_table_data)
        df_year_products_data = pd.DataFrame.from_dict(year_products_data)

        df = pd.merge(
            df_year_table_data, df_year_products_data, on="submissionIdentifier", how="left"
        )
        df["year"] = year

        # convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        # filename and upload
        filename = f"{table_id}".lower().replace(" ", "")

        logger.info(f"Successfully extracted {df.shape[0]} rate review records for {table_id}")

        return csv_bytes, filename, FileType.CSV

    def _build_data_url(self, state, year) -> str:
        """
        Builds necessary url for data extraction
        """
        return f"{self.PAGE_URL}ratereviewservices/urr/submissions?state={state}&year={year}&marketType={self.MARKET_TYPE}"

    def _try_get_table(self, search_info: dict) -> list[dict]:
        """
        Attempt to get tables from the raw data sources
        """
        try:
            url = search_info.get("data_url")
            response = requests.get(url)
            response.raise_for_status()
            table = response.json()
            if not table:
                logger.debug("No table found in response")
                return []

            submissions = table.get("submissionsList", [])
            if not submissions:
                logger.debug("No submissions found in table")

            return submissions

        except Exception as e:
            logger.debug(f"Extraction failed: {e}")
            return []

    def _try_get_products_info(self, table_info: list[dict]) -> list[dict]:
        """
        Attempt to get subtables for each product
        """
        try:
            # in order to get here, len(table) > 0, so no need to check
            full_products_data = []
            for item in table_info:
                time.sleep(self.SLEEP_TIMER)  # server overloading protection
                # gathering submissionIdentifier as a link between tables
                submissions_identifier = item["submissionIdentifier"]
                response = requests.get(
                    f"https://ratereview.healthcare.gov/ratereviewservices/urr/products?submissionID={submissions_identifier}"
                )
                response.raise_for_status()
                products_table = response.json()
                if not products_table:  # this should never happen, since there must be a product for the sample to be on the table
                    logger.debug(
                        f"No products found at submissionsIdentifier {submissions_identifier}"
                    )
                    continue

                # getting the info from the dictionary 'products' and adding submissionIdentifier as a merging factor
                sub_product_data = products_table.get("products", [])
                sub_product_data = [
                    {"submissionIdentifier": submissions_identifier, **product}
                    for product in sub_product_data
                ]
                full_products_data += sub_product_data

            return full_products_data

        except Exception as e:
            logger.debug(f"Extraction failed: {e}")
            return []
