from utils.logger import get_logger
from db_init import run_ddl
from ingestion.bronze_orchestrator import run_ingestion
from transformation.silver.silver_orchestrator import run_transformation

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("================Run main.py===============")
    # run_ddl()
    # run_ingestion()
    run_transformation()