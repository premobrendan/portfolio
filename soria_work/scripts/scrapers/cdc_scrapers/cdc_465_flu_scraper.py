import json
import logging
import time
import zipfile
from io import BytesIO, StringIO

import pandas as pd
import requests
from sqlalchemy.orm import Session

from soria_api.diglet.base import BaseScraper
from soria_api.diglet.types import DownloadResult, SourceData
from soria_api.models import Source
from soria_api.models.source import APILoadStrategy, SourceType
from soria_api.models.source_file import FileType, SourceFile

logger = logging.getLogger(__name__)


class CDC465FluScraper(BaseScraper):
    """CDC Flu Data Scraper"""

    SOURCE_TYPE = SourceType.API
    API_LOAD_STRATEGY = APILoadStrategy.TRUNCATE
    IS_PARENT = False

    METADATA_URL = "https://gis.cdc.gov/grasp/flu2/GetPhase02InitApp?appVersion=Public"
    DATA_URL = "https://gis.cdc.gov/grasp/flu2/PostPhase02DataDownload"

    # these headers were reverse-engineered from browser requests to the CDC GIS portal
    DATA_HEADERS = {
        "Origin": "https://gis.cdc.gov",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://gis.cdc.gov/grasp/fluview/fluportaldashboard.html",
        "User-Agent": "cdcfluview Python package",
    }

    DATA_PAYLOAD = {
        "AppVersion": "Public",
        "DatasourceDT": [{"ID": 1, "Name": "ILINet"}],
    }

    # id and len are based on information found on the cdc site
    REGION_MAP = [
        {"region_name": "national", "region_id": 3, "region_len": 1},
        {"region_name": "hhs", "region_id": 1, "region_len": 10},
        {"region_name": "census", "region_id": 2, "region_len": 9},
        {"region_name": "state", "region_id": 5, "region_len": 59},
    ]

    SLEEP_TIMER = 0.5  # not really worried about overflow, mostly exists to dissuade errors

    @property
    def source_key(self) -> str:
        """Unique identifier for this source"""
        return "CDC_465_flu"

    @property
    def source_name(self) -> str:
        return "cdc 465 Flu"

    @property
    def scraper_id(self):
        return "CDC_465"

    def fetch_raw_data(self) -> list[dict]:
        """Get available seasons from CDC"""
        logger.info("Finding available CDC flu data")

        try:
            # seasons data
            response = requests.get(self.METADATA_URL)
            response.raise_for_status()
            data = response.json()

            # inits for below
            available_data = []
            max_season = 0
            min_season = 50  # much higher than the lowest season
            label = None

            # getting max and min of seasons, mmwrid
            for season in data.get("seasons", []):
                if not season["enabled"]:
                    continue

                if max_season < season["seasonid"]:
                    max_season = season["seasonid"]
                    label = season["label"]

                min_season = min(min_season, season["seasonid"])

            if not label:
                logger.debug("No label found in data; likely no data found")

            available_data.append(
                {
                    "season_id_max": max_season,
                    "season_id_min": min_season,
                    "season_label": label,
                    "file_url": self.DATA_URL,
                }
            )

            logger.info(f"Data found for year range {1960 + min_season}-{1960 + max_season}")
            return available_data

        except requests.RequestException as e:
            logger.error(f"Error fetching available data: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Error fetching json: {e}")
            return []
        except Exception as e:
            logger.error(f"An unexpected error occurred in get_available_data: {e}")
            return []

    def find_new_data(self, available: list[SourceData], db_session: Session) -> list[SourceData]:
        """We are truncating, so just return available here"""

        source = db_session.query(Source).filter(Source.name == self.source_key).one_or_none()
        if not source:
            logger.error(f"Source not found: {self.source_key}")
            return []

        existing_files = (
            db_session.query(SourceFile).filter(SourceFile.source_id == source.id).all()
        )
        self.OLD_DATA = pd.DataFrame(existing_files)

        return available

    def download_files(self, data: SourceData) -> DownloadResult:
        """Download file content."""
        season_id_max = data.metadata["season_id_max"]
        season_id_min = data.metadata["season_id_min"]
        logger.info(
            f"Downloading data for years in range {1960 + season_id_min}-{1960 + season_id_max}"
        )

        # getting all season info for all years by region
        df = pd.DataFrame()
        for region_dict in self.REGION_MAP:
            time.sleep(self.SLEEP_TIMER)
            # setting up region specifics for payload
            region_data_payload = self.DATA_PAYLOAD.copy()

            # gathering all years available in payload
            region_data_payload["SeasonsDT"] = [
                {"ID": i, "Name": str(i)} for i in range(season_id_min, season_id_max + 1)
            ]

            # gathering region information in payload
            region_data_payload["RegionTypeId"] = region_dict["region_id"]
            if region_dict["region_name"] == "national":
                region_data_payload["SubRegionsDT"] = [{"ID": 1, "Name": ""}]
            else:
                region_data_payload["SubRegionsDT"] = [
                    {"ID": i, "Name": str(i)} for i in range(1, region_dict["region_len"] + 1)
                ]

            # Downloading and Combining
            region_df = self._download_data(region_dict["region_name"], region_data_payload)
            if region_df.empty or region_df is None:
                logger.debug(f"No data able to be downloaded for {region_dict['region_name']}")
                continue
            else:
                df = pd.concat([df, region_df], ignore_index=True)

        if df.empty:
            raise ValueError("No data was successfully downloaded for any region")

        # convert to csv
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        filename = f"CDC_465_seasons_{season_id_min}-{season_id_max}".lower().replace(" ", "")
        file_type = FileType.CSV

        return DownloadResult.from_content(csv_bytes, filename, file_type)

    def _download_data(self, region: str, payload: dict) -> pd.DataFrame:
        """Download, Unzip and Pandas the ILINET CSV"""

        try:
            response = requests.post(self.DATA_URL, headers=self.DATA_HEADERS, json=payload)
            response.raise_for_status()
            logger.info(f"Successfully downloaded {region} data")

            buffer = BytesIO(response.content)

            # unzip the content and get ILINet
            with zipfile.ZipFile(buffer, "r") as zf:
                # Get the first (and should be the only) file in the ZIP
                csv_filename = None
                for file_info in zf.infolist():
                    if file_info.filename.endswith(".csv"):
                        csv_filename = file_info.filename
                        break  # get the first CSV

                if not csv_filename:
                    logger.error(f"No CSV file found in downloaded ZIP for region {region}")
                    return self.OLD_DATA[self.OLD_DATA["region_type"] == region]
                csv_content = zf.read(csv_filename)

                # make df and update to proper region to df
                df = pd.read_csv(BytesIO(csv_content), skiprows=1)

            if region == "hhs" and "REGION" in df.columns:
                df["REGION"] = df["REGION"].apply(lambda x: f"{x}")
            elif region == "national" and "REGION" in df.columns:
                df["REGION"] = region  # is "X" otherwise

            logger.info(f"Successfully compiled data for {region} to DF with {df.shape[0]} rows")
            return df

        except requests.RequestException as e:
            logger.error(f"Error during download or request for region {region}: {e!s}")
            return self.OLD_DATA[self.OLD_DATA["REGION TYPE"] == region]  # old data fallback
        except zipfile.BadZipFile as e:
            logger.error(f"Downloaded file is not a valid ZIP for region {region}: {e!s}")
            return self.OLD_DATA[self.OLD_DATA["REGION TYPE"] == region]  # old data fallback
        except pd.errors.EmptyDataError:
            logger.warning("CSV file is empty. Skipping.")
            return self.OLD_DATA[self.OLD_DATA["REGION TYPE"] == region]  # old data fallback
        except Exception as e:
            logger.error(
                f"An unexpected error occurred in _download_data for region {region}: {e!s}"
            )
            return self.OLD_DATA[self.OLD_DATA["REGION TYPE"] == region]  # old data fallback
