import utils.db as db
from utils.logger import get_logger
from transformation.silver.silver_transformer.extractor import Extractor
from transformation.silver.silver_transformer.cleaner import Cleaner
from transformation.silver.silver_transformer.normalizer import Normalizer

import pandas as pd

logger = get_logger(__name__)
    

def run_transformation():
    logger.info("running transformation")
    engine = db.get_sqlalchemy_engine() 
    try:
        # Read raw json from bronze.anime_raw
        query = "SELECT payload FROM bronze.anime_raw"
        payload = pd.read_sql(query,engine)

        # Extract data from bronze
        silver_schema = Extractor(payload).run_extraction()
        # Clean Data
        cleaned_silver_schema = Cleaner(silver_schema).run_clean()
        # normalize
        normalized_silver_schema = Normalizer(cleaned_silver_schema).run_normalization()
        logger.info("**Transformation Successfully**")
        return normalized_silver_schema

    except Exception as e:
        logger.error(f"**Error running transformation: {e}**")
