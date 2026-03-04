from ingestion.jikan_ingestor import JikanIngestor
from loader.bronze_loader import BronzeLoader
from utils.logger import get_logger

logger = get_logger(__name__)
class BronzeOrchestrator:
    def __init__(self, db_name = "animed"):
        self.bronze_ingestor = JikanIngestor()
        self.bronze_loader = BronzeLoader(db_name)

    def run_bronze_ingestion(self):
        try:

            for df_anime_raw, df_anime_pagination_log in self.bronze_ingestor.run_ingestion():
                self.bronze_loader.load_bronze(df_anime_raw, df_anime_pagination_log)
                
        except Exception as e:
            logger.error(f"Ingestion Failed{e}")
            
        finally:
            self.bronze_loader.close()