from ingestion.bronze_ingestor.jikan_ingestor import JikanIngestor
from loader.bronze_loader import BronzeLoader
from utils.logger import get_logger

logger = get_logger(__name__)

def run_ingestion():
    try:
        bronze_ingestor = JikanIngestor()
        bronze_loader = BronzeLoader("animed")

        for df_anime_raw, df_anime_pagination_log in bronze_ingestor.run_ingestion():
            bronze_loader.load_bronze(df_anime_raw, df_anime_pagination_log)
            
    except Exception as e:
        logger.error(f"Ingestion Failed{e}")
        
    finally:
        bronze_loader.close()