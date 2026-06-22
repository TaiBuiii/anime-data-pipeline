from utils.logger import get_logger
from transformation.silver.extractor import Extractor
from transformation.silver.cleaner import Cleaner
from transformation.silver.normalizer import Normalizer
from loader.silver_loader import SilverLoader

import pandas as pd

logger = get_logger(__name__)
    
class SilverOrchestrator:
    """
    Orchestrates the Silver layer cleaning transformations .
    
    This class will extract anime data from raw JSON in bronze layer, clean the data, and ultimately normalize the data.
    """
    def __init__(self, db_name : str = "animedw"):
        self.silver_loader : SilverLoader = SilverLoader(db_name)

    def run_silver_transformation(self):
        logger.info("**transforming silver**")
        try:
            query = "SELECT payload FROM bronze.anime_raw"

            # read the data from bronze layer as DataFrame
            payload = self.silver_loader.db_manager.query_dataframe(query)

            # Extract data from bronze
            silver_schema = Extractor(payload).run_extraction()

            # Clean Data
            cleaned_silver_schema = Cleaner(silver_schema).run_clean()

            # normalize
            normalized_silver_schema = Normalizer(cleaned_silver_schema).run_normalization()

            # load silver
            self.silver_loader.load_silver(normalized_silver_schema)
            
            logger.info("**Transforming silver Successfully**")

        except Exception as e:
            logger.error(f"**Error running transformation: {e}**", exc_info=True)
            raise

