from utils.logger import get_logger
from db_init import DatabaseInitializer
from orchestrator.bronze_orchestrator import BronzeOrchestrator
from orchestrator.silver_orchestrator import SilverOrchestrator
from orchestrator.gold_orchestrator import GoldOrchestrator
logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("================Run main.py===============")
    # DatabaseInitializer().run_ddl()
    # BronzeOrchestrator().run_bronze_ingestion()
    # SilverOrchestrator().run_silver_transformation()
    GoldOrchestrator().run_gold_transformation()

    
    