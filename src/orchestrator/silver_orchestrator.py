from utils.logger import get_logger
from transformation.silver.extractor import Extractor
from transformation.silver.cleaner import Cleaner
from transformation.silver.normalizer import Normalizer
from loader.silver_loader import SilverLoader

import pandas as pd

logger = get_logger(__name__)
    
class SilverOrchestrator:
    def __init__(self, db_name : str = "animed"):
        self.silver_loader : SilverLoader = SilverLoader(db_name)

    def run_silver_transformation(self):
        logger.info("running transformation")
        try:
            query = "SELECT payload FROM bronze.anime_raw"
            payload = self.silver_loader.db_manager.query_dataframe(query)
            # Extract data from bronze
            silver_schema = Extractor(payload).run_extraction()

            # Clean Data
            cleaned_silver_schema = Cleaner(silver_schema).run_clean()

            # normalize
            normalized_silver_schema = Normalizer(cleaned_silver_schema).run_normalization()

            # load silver
            self.silver_loader.load_silver(normalized_silver_schema)
            
            logger.info("**Transformation Successfully**")
            # return normalized_silver_schema

        except Exception as e:
            logger.error(f"**Error running transformation: {e}**")
        finally:
            self.silver_loader.close()
