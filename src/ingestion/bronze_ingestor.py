from ingestion.jkan_client import fetch_page_data
from utils.logger import get_logger
from loaders.bronze_loader import load_bronze

import pandas as pd
import json
logger = get_logger(__name__)

def extract_pagination(data : dict) -> list:
    return [(
        data["current_page"],
        data["last_visible_page"],
        data["has_next_page"],
        data["items"]["count"],
        data["items"]["total"],
        data["items"]["per_page"],
    )]

def extract_anime_raw(data : list, page : int) -> list:
    for record in data:
        return [
            (record["mal_id"], page, record) for record in data
        ]
    
def run_ingestion(startPage : int = 1):
    page = startPage
    while True:
        logger.info (f"Fetching: {page}")
        data = fetch_page_data(page)
        pagination = extract_pagination(data["pagination"])
        anime_raw = extract_anime_raw(data["data"],page)
        load_bronze(anime_raw, pagination)
        has_next_page = data["pagination"]["has_next_page"]
        if not has_next_page:
            logger.info("No more pages. Stopping.")
            break
        page += 1
        

