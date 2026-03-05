from utils.db import DatabaseManager
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)

class GoldOrchestrator:
    def __init__(self):
        self.db_manager = DatabaseManager("animedw")
        self.TRANSFORM_PATH  = Path(__name__).resolve().parent / "src" / "transformation" / "gold"
    
    def run_gold_transformation(self):
        try:
            for file in self.TRANSFORM_PATH.iterdir():
                logger.info(f"Creating {file.name}")
                self.db_manager.execute_file(file)

            logger.info("**Transforming gold successfully**")
        except Exception as e:
            logger.error(f"**Failed running gold transformation: {e}**")
    