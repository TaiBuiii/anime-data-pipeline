from ingestion.jikan_client import fetch_page_data
from utils.logger import get_logger
from loaders.bronze_loader import load_bronze
from ingestion.bronze_extractor import extract_anime_raw, extract_pagination
import utils.db as db

import json
logger = get_logger(__name__)

    
def run_ingestion(startPage : int = 1):
    """
    Acts as the orchestration for the extraction phase. It calls extract_pagination() and 
    extract_anime_raw() for extraction and load_bronze() for bronze insertion.

    Args
    startPage : the page where user want to start fetching. by default, startPage = 1
    """
    total_record = 0
    success = True
    page = startPage
    conn = db.get_animedw_connection()
    while True:
        try:
            logger.info (f"Fetching: {page}")

            # Extract raw json from the specified page
            data = fetch_page_data(page)

            # Extract a record for bronze.anime_pagination_log
            pagination = extract_pagination(data["pagination"])

            # Extract records for bronze.anime_raw
            anime_raw = extract_anime_raw(data["data"],page)
            # Load immediate to bronze layer when a page is fetched
            load_bronze(anime_raw, pagination, conn)
            # Stop when there is no page left
            has_next_page = data["pagination"]["has_next_page"]
            if not has_next_page:
                logger.info("No more pages. Stopping.")
                break

            # Else keep fetching the next page
            total_record += len(anime_raw)
            page += 1
            logger.info(f"Currently Inserted {total_record} rows from {page} pages")
            logger.info("===========================================================================")

        except Exception as e:
            logger.error(f"Ingestion interupted: {e}")
            success = False
            break
    
    conn.close()

    conn.close()
    if success:
        logger.info("=================Load bronze completed successfully================")

    else:
        logger.warning("===============Load bronze terminated early======================")
        

