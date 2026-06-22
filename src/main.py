from utils.logger import get_logger
from db_init import DatabaseInitializer
from orchestrator.bronze_orchestrator import BronzeOrchestrator
from orchestrator.silver_orchestrator import SilverOrchestrator
from orchestrator.gold_orchestrator import GoldOrchestrator
logger = get_logger(__name__)

def main():
    logger.info("================Run main.py===============")

    # initialize database 
    DatabaseInitializer().run_ddl()

    # store raw data in bronze layer 
    BronzeOrchestrator().run_bronze_ingestion()

    # perform basic data transformation 
    SilverOrchestrator().run_silver_transformation()

    
    GoldOrchestrator().run_gold_transformation()

    
if __name__ == "__main__":
    main()

    
    