from utils.logger import get_logger
import utils.db as db

import pandas as pd
import requests
import time
import json


logger = get_logger(__name__)

class JikanIngestor:
    RETRY_ATTEMPTS = 10
    TIMEOUT = 10
    SLEEP_TIME = 2

    def __init__(self, start_page : int = 1):
        self.BASE_URL = "https://api.jikan.moe/v4"
        self.URL = f"{self.BASE_URL}/anime"
        self.start_page = start_page
        self.logger = logger
    
    def _fetch_page_data(self, page : int):
        params = {"page" : page}
        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                response = requests.get (self.URL, params = params, timeout = self.TIMEOUT)

                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 429:
                    self.logger.warning(f"Rate Limited on page {page}. Sleeping 2s...")
                    time.sleep(self.SLEEP_TIME)
                
            except Exception as e:
                self.logger.error(f"Failed attempt {attempt + 1} fetching page {page}: {e}")

        raise Exception(f" Max retries exceeded for page {page}")

    @staticmethod
    def _ingest_pagination(data : dict) -> list[tuple]:
        """
        This function is called by run_ingestion() to ingest the metadata in each page,
        as a list of tuple. This returned datatype facilitates the loading process

        It returns data for bronze.anime_pagination_log
        """
        return [{
            "page" : data["current_page"],
            "last_visible_page" : data["last_visible_page"],
            "has_next_page" : data["has_next_page"],
            "items_count" : data["items"]["count"],
            "items_total" : data["items"]["total"],
            "items_per_page" : data["items"]["per_page"],
        }]
        
    @staticmethod
    def _ingest_anime_raw(data : list[dict], page : int) -> list[tuple]:
        """
        This function is called by run_ingestion() to ingest all anime records,
        contained in the page, as a list of tuple. This returned datatype 
        facilitates the loading process

        It returns data for bronze.anime_raw
        """
        return [{
            "mal_id" : record["mal_id"],
            "page" : page, 
            "payload" : json.dumps(record)}
            for record in data]
    
    def run_ingestion(self):
        """
        Acts as the orchestration for the ingestion phase. It calls ingest_pagination() and 
        ingest_anime_raw() for ingestion.

        Args
        startPage : the page where user want to start fetching. by default, startPage = 1
        """
        # initialize variable
        page : int = self.start_page

        # loop through pages to get data
        while True:
            try:
                logger.info (f"Fetching page {page}")

                # Extract raw json from the specified page
                data = self._fetch_page_data(page)

                # Extract a record for bronze.anime_pagination_log
                df_anime_pagination_log = pd.DataFrame(self._ingest_pagination(data["pagination"]))

                # Extract records for bronze.anime_raw
                df_anime_raw = pd.DataFrame(self._ingest_anime_raw(data["data"],page))

                #  throw data out 
                yield df_anime_raw, df_anime_pagination_log
                
                # Stop when there is no page left
                if not data["pagination"]["has_next_page"]:
                    logger.info("No more pages. Stopping.")
                    break
                    
                # Else keep fetching the next page
                page += 1

            except Exception as e:
                logger.error(f"Ingestion interupted: {e}")
                break



            

