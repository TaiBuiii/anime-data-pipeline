from utils.logger import get_logger
from db_init import DatabaseInitializer
from orchestrator.stage_orchestrator import StageOrchestrator
from orchestrator.edw_orchestrator import EdwOrchestrator
from orchestrator.datamart_orchestrator import DataMartOrchestrator
logger = get_logger(__name__)

def main():
    logger.info("================Run main.py===============")

    # initialize database 
    DatabaseInitializer().run_ddl()

    # store raw data in stage layer 
    StageOrchestrator().run_stage_ingestion()

    # perform basic data transformation 
    EdwOrchestrator().run_edw_transformation()

    
    DataMartOrchestrator().run_datamart_transformation()

    
if __name__ == "__main__":
    main()

    
    