from utils.logger import get_logger
from transformation.edw.extractor import Extractor
from transformation.edw.cleaner import Cleaner
from transformation.edw.normalizer import Normalizer
from loader.edw_loader import EdwLoader

import pandas as pd

logger = get_logger(__name__)
    
class EdwOrchestrator:
    """
    Orchestrates the edw layer cleaning transformations .
    
    This class will extract anime data from raw JSON in stage layer, clean the data, and ultimately normalize the data.
    """
    def __init__(self, db_name : str = "animedw"):
        self.edw_loader : EdwLoader = EdwLoader(db_name)

    def run_edw_transformation(self):
        logger.info("**transforming edw**")
        try:
            query = "SELECT payload FROM stage.anime_raw"

            # read the data from stage layer as DataFrame
            payload = self.edw_loader.db_manager.query_dataframe(query)

            # Extract data from stage
            edw_schema = Extractor(payload).run_extraction()

            # Clean Data
            cleaned_edw_schema = Cleaner(edw_schema).run_clean()

            # normalize
            normalized_edw_schema = Normalizer(cleaned_edw_schema).run_normalization()

            # load edw
            self.edw_loader.load_edw(normalized_edw_schema)
            
            logger.info("**Transforming edw Successfully**")

        except Exception as e:
            logger.error(f"**Error running transformation: {e}**", exc_info=True)
            raise

