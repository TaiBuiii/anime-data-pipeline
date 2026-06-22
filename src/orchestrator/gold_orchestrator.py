from utils.db import DatabaseManager
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)

class GoldOrchestrator:
    """Orchestrates the Gold layer analytical transformations by executing SQL scripts.
    
    This class scans a transformation directory and sequentially executes 
    all SQL files against the target database warehouse to construct final data models, 
    aggregations, and business-ready analytical tables (Gold layer).
    """
    def __init__(self):
        """Initializes the orchestrator by instantiating the database manager and resolving paths."""
        self.gold_loader = DatabaseManager("animedw")

        # Resolve the absolute path to the directory containing Gold SQL transformation scripts
        self.TRANSFORM_PATH  = Path(__file__).resolve().parent.parent / "transformation" / "gold"
    
    def run_gold_transformation(self):
        """Iterates through and sequentially runs all SQL transformation scripts in the Gold directory.
        
        This loop automatically picks up any SQL definition file placed inside the target 
        directory path and pipes it directly to the DatabaseManager executing stack.
        
        Raises:
            Exception: Escalates any script runtime or connection exceptions to safely 
                       fail the pipeline orchestration step.
        """
        logger.info("**Transforming gold**")
        try:
            # Loop through all files present inside the Gold transformation directory
            for file in self.TRANSFORM_PATH.iterdir():
                logger.info(f"Creating {file.name}")

                # Read and execute the plain-text query scripts against the database context
                self.gold_loader.execute_file(file)

            logger.info("**Transforming gold successfully**")
        except Exception as e:
            logger.error(f"**Failed gold transformation: {e}**")
            raise

    