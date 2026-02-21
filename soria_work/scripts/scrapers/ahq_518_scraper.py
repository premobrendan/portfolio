import logging
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from soria_api.diglet.base import BaseScraper
from soria_api.diglet.types import DownloadResult, SourceData
from soria_api.models import Source
from soria_api.models.source import SourceType
from soria_api.models.source_file import SourceFile

logger = logging.getLogger(__name__)


class AHQ518HospitalCompendiumData(BaseScraper):
    """Gets Hospital Compendium Data"""

    BASE_URL = "https://www.ahrq.gov"
    PAGE_URL = "https://www.ahrq.gov/chsp/data-resources/compendium.html#datafiles"
    SOURCE_TYPE = SourceType.FILE

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    DESIRED_EXTENSIONS = [".csv", ".xlsx"]  # ordered by priority

    @property
    def source_key(self) -> str:
        return "AHQ_518_hospital_compendium_data"

    @property
    def source_name(self) -> str:
        return "ahq 518 hospital compendium data"

    @property
    def scraper_id(self):
        return "AHQ_518"

    def _subpage_scraper(self, sublink: str, year: int) -> list[dict]:
        """Gets data from a subpage"""
        try:
            return_list = []
            seen_sublinks = set()  # only sees download links

            response = requests.get(sublink, headers=self.HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            data_section = None
            for header in soup.find_all(["h2"]):
                data_section = header.find_next_sibling()

            if not data_section:
                logger.warning(f"Could not find data on page: {sublink}")
                return []

            links = data_section.find_all("a", href=True)

            for ext in self.DESIRED_EXTENSIONS:  # starts with ".csv", then ".xlsx"
                for link in links:
                    href_string = link["href"]
                    if href_string and href_string.endswith(ext):
                        # getting extensionless file name to avoid getting file extension independent dupes
                        split_on_period = href_string.split(".")
                        if len(split_on_period) == 2:
                            no_extension = split_on_period[0]
                        else:
                            no_extension = ".".join(split_on_period[:-1])

                        # skip duplicates
                        if no_extension in seen_sublinks:
                            continue
                        seen_sublinks.add(no_extension)  # just add extensionless file name

                        # url maker and beautify name
                        url = (
                            href_string
                            if href_string.startswith("http")
                            else urljoin(self.BASE_URL, href_string)
                        )
                        better_name = href_string.split("/")[-1]

                        # add to return list
                        return_list.append({"name": better_name, "file_url": url})

            logger.info(f"Successfully gathered {len(seen_sublinks)} data links from {sublink}")
            return return_list

        except Exception as e:
            logger.exception(f"Error finding data: {e}")
            return []

    def fetch_raw_data(self) -> list[dict]:
        """Gets all available data from site"""

        try:
            seen_links = set()  # only sees subpage links
            raw_data = []

            # get data
            response = requests.get(self.PAGE_URL, headers=self.HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            content_div = None
            for header in soup.find_all(["h1"]):
                if header.text.strip() == "Compendium of U.S. Health Systems":
                    content_div = header.find_next_sibling("div", class_="clearfix")
                    break

            if not content_div:
                logger.warning(f"No links to subpages found at {self.PAGE_URL}")
                return []

            for link in content_div.find_all("li"):
                link_text = link.text.strip().split()
                # if link isnt a Compendium, we dont want it
                if link_text[1] != "Compendium":
                    continue
                # metadata
                year_number = int(link_text[0])
                # get data from subpage
                href = link.find("a")
                if href:
                    download_link = urljoin(self.BASE_URL, href["href"])
                    # no dupes
                    if download_link in seen_links:
                        continue
                    else:
                        seen_links.add(download_link)
                        raw_data += self._subpage_scraper(download_link, year_number)

            logger.info(f"Successfully gathered {len(raw_data)} files")
            return raw_data

        except Exception as e:
            logger.exception(f"Error finding data: {e}")
            return []

    def find_new_data(self, available: list[SourceData], db_session: Session) -> list[SourceData]:
        """Find new data by comparing filenames using the format"""
        logger.info(f"Finding new data for {self.source_name}")

        # Get source record
        source = db_session.query(Source).filter(Source.name == self.source_key).one_or_none()

        # Get all existing files for this source
        existing_files = (
            db_session.query(SourceFile).filter(SourceFile.source_id == source.id).all()
        )

        # Create a set of existing filenames for fast lookup
        existing_filenames = {file.file_name for file in existing_files}
        if not available:
            logger.info("No files found during fetch ('available' list is empty).")
            return []
        logger.debug(
            f"Comparing against {len(existing_filenames)} existing filenames in DB for source {self.source_key}."
        )

        # Find files we don't have yet
        new_data = []

        for data in available:
            try:
                # 1. Get the base name (e.g., "filename-parentdir") generated earlier
                # Our naming convention uses this name for the file name, so we only need to check this
                filename_to_compare = data.metadata.get("name")
                if not filename_to_compare:
                    logger.warning(f"Skipping item: 'name' missing in metadata: {data.metadata}")
                    continue

                # Compare the constructed filename against existing ones
                if filename_to_compare not in existing_filenames:
                    new_data.append(data)
                    logger.info(f"Found new file: {filename_to_compare}")

            except Exception as e:
                logger.error(f"Error processing available data item {data.metadata}: {e}")
                continue  # Skip to next item on error

        logger.info(f"Found {len(new_data)} new files to download for source {self.source_key}")
        return new_data

    def download_files(self, data: SourceData) -> DownloadResult:
        """Download file content."""
        file_name = data.metadata.get("name")  # use this for file name
        file_url = data.metadata.get("file_url")

        # get data and get extension
        response = requests.get(file_url, headers=self.HEADERS)
        response.raise_for_status()
        file_content = response.content

        file_type = self.detect_file_type(response, file_name)

        return DownloadResult.from_content(file_content, file_name, file_type)
