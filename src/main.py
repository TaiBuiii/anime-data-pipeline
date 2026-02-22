from utils.logger import get_logger
from db_init import run_ddl
from ingestion.anime_ingestor import run_ingestion

logger = get_logger(__name__)
if __name__ == "__main__":
    logger.info("Run main.py")
    run_ddl()
    run_ingestion()