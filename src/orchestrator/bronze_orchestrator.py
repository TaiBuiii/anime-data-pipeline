from ingestion.jikan_ingestor import JikanIngestor
from loader.bronze_loader import BronzeLoader
from utils.logger import get_logger

logger = get_logger(__name__)
class BronzeOrchestrator:
    def __init__(self, db_name = "animedw"):
        self.bronze_ingestor = JikanIngestor()
        self.bronze_loader = BronzeLoader(db_name)

    def run_bronze_ingestion(self):
        logger.info("**Ingesting bronze**")
        try:
            total_anime : int = 0
            for df_anime_raw, df_anime_pagination_log in self.bronze_ingestor.run_ingestion():
                self.bronze_loader.load_bronze(df_anime_raw, df_anime_pagination_log)

                total_anime += len(df_anime_raw)
                logger.info(f"Currently loaded {total_anime}")

            logger.info("**Ingesting bronze successfully**")
        except Exception as e:
            logger.error(f"**Ingestion Failed{e}**")
            raise
