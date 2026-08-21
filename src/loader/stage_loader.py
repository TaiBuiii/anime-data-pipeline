import pandas as pd
from loader.base_loader import BaseLoader
from utils.logger import get_logger

logger = get_logger(__name__)
class StageLoader(BaseLoader):
    """
    This is the loader for stage layer, it will load anime_pagination_log, and anime data into stage layer as raw JSON
    """
    def __init__(self, db_anime : str = "animedw"):
        super().__init__(db_name = db_anime)

        
    def load_stage(self, anime_raw : pd.DataFrame, anime_pagination_log : pd.DataFrame):
        """
        This function acts as the orchestrator which calls insert_anime_raw() 
        and insert_anime_pagination_log() to perform insertion

        """
    
        try:
            self.db_manager.load_table(table_name="anime_raw", schema = "stage", df = anime_raw)
            self.db_manager.load_table(table_name="anime_pagination_log", schema = "stage", df = anime_pagination_log)
            logger.info(f"Load Successfully {len(anime_raw)}")

        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise

