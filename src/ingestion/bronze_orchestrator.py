from ingestion.bronze_ingestor.jikan_ingestor import JikanIngestor
from loader.bronze_loader import BronzeLoader
from utils.logger import get_logger

logger = get_logger(__name__)

def run_ingestion():
    try:
        bronze_ingestor = JikanIngestor()
        bronze_loader = BronzeLoader()

        for anime_data, pagination_data in bronze_ingestor.run_ingestion():
            bronze_loader.load_bronze(anime_data, pagination_data)
            
    except Exception as e:
        logger.error(f"Ingestion Failed{e}")
        
    finally:
        bronze_loader.close()