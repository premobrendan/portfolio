import logging
from io import StringIO

import pandas as pd
import requests
from sqlalchemy.orm import Session

from soria_api.diglet.base import BaseScraper
from soria_api.diglet.scrapers.cdc_scrapers.__collection import CDC_API_CHILD_SCRAPERS
from soria_api.diglet.types import DownloadResult, SourceData
from soria_api.models.source import APILoadStrategy, SourceType
from soria_api.models.source_file import FileType

logger = logging.getLogger(__name__)


class CDCScraper(BaseScraper):
    """
    scraper for CDC resources
    """

    IS_PARENT = True
    CHILD_SCRAPERS = CDC_API_CHILD_SCRAPERS
    API_LOAD_STRATEGY = APILoadStrategy.TRUNCATE
    SOURCE_TYPE = SourceType.API

    def __init__(self, scraper_config):
        self.name = scraper_config.name
        self.title = scraper_config.title
        self.resource_id = scraper_config.resource_id
        self.id = scraper_config.scraper_id
        self.BASE_URL = "https://data.cdc.gov"
        self.CSV_URL = f"{self.BASE_URL}/resource/{self.resource_id}.csv"
        self.METADATA_URL = f"{self.BASE_URL}/api/views/{self.resource_id}"
        self.PARAMS = {
            "$limit": 1000,
            "$offset": 0,
            "$order": ":id",  # Ensures stable, reproducible ordering
        }

    @property
    def source_key(self) -> str:
        """Return the key for this source"""
        return self.name

    @property
    def source_name(self) -> str:
        """Return the human-readable name for this source"""
        return self.title

    @property
    def scraper_id(self) -> str:
        return self.id

    def download_files(self, data: SourceData) -> DownloadResult:
        """Download file content."""
        all_dfs = []
        params = self.PARAMS.copy()  # Create a copy to avoid modifying the original

        while True:
            response = requests.get(self.CSV_URL, params=params)
            response.raise_for_status()

            # Convert CSV response to DataFrame
            df = pd.read_csv(StringIO(response.text))
            # Replace empty strings with NaN
            df = df.replace("", pd.NA)
            df = df.replace(" ", pd.NA)

            if df.empty:
                break  # Stop if no more data is returned

            all_dfs.append(df)
            params["$offset"] += params["$limit"]  # Move to the next batch

        final_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(final_df)

        # Convert DataFrame to CSV bytes
        csv_buffer = StringIO()
        final_df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        filename = self.name
        file_type = FileType.CSV

        return DownloadResult.from_content(csv_bytes, filename, file_type)

    def fetch_raw_data(self) -> list[dict]:
        """
        Fetch raw data from the source. Returns a list of dictionaries with:
        - file_url: URL to download the file
        - metadata specific to the source
        """
        response = requests.get(self.METADATA_URL)
        response.raise_for_status()

        metadata = response.json()

        return [
            {
                "file_url": self.CSV_URL,
                "resource_id": self.resource_id,
                "filename": f"{self.source_key}.csv",
                "description": metadata.get("description", ""),
                "last_updated": metadata.get("updatedAt", ""),
            }
        ]

    def find_new_data(self, available: list[SourceData], db_session: Session) -> list[SourceData]:
        """
        Find new data that needs to be scraped. For CDC data, we:
        1. Check the last update time from the metadata
        2. Compare with our last scraped version
        3. Return data for scraping if it's new or updated
        """
        # Get the metadata to check last update time
        metadata = self.fetch_raw_data()

        source = available[0]

        if source.metadata["last_updated"] <= metadata[0].get("last_updated", ""):
            return [source]
        else:
            return []
