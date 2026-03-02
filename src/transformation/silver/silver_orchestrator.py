import utils.db as db
from utils.logger import get_logger
from transformation.silver.silver_transformer.extractor import Extractor
from transformation.silver.silver_transformer.cleaner import Cleaner
from transformation.silver.silver_transformer.normalizer import Normalizer
from loader.silver_loader import SilverLoader
from loader.silver_loader import SilverLoader

import pandas as pd

logger = get_logger(__name__)
    

def run_transformation():
    logger.info("running transformation")
    silver_loader = SilverLoader("animed")
    try:
        # Read raw json from bronze.anime_raw
        query = "SELECT payload FROM bronze.anime_raw"
        payload = silver_loader.db_manager.query_dataframe(query)

        # Extract data from bronze
        silver_schema = Extractor(payload).run_extraction()
        # Clean Data
        cleaned_silver_schema = Cleaner(silver_schema).run_clean()
        # normalize
        normalized_silver_schema = Normalizer(cleaned_silver_schema).run_normalization()
        # load silver
        silver_loader.load_silver(normalized_silver_schema)
        
        logger.info("**Transformation Successfully**")
        # return normalized_silver_schema

    except Exception as e:
        logger.error(f"**Error running transformation: {e}**")
    finally:
        silver_loader.close()
