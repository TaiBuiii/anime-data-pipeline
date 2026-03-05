from utils.db import DatabaseManager
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)

class GoldOrchestrator:
    def __init__(self):
        self.gold_loader = DatabaseManager("animedw")
        self.TRANSFORM_PATH  = Path(__name__).resolve().parent.parent / "transformation" / "gold"
    
    def run_gold_transformation(self):
        logger.info("**Transforming gold**")
        try:
            for file in self.TRANSFORM_PATH.iterdir():
                logger.info(f"Creating {file.name}")
                self.gold_loader.execute_file(file)

            logger.info("**Transforming gold successfully**")
        except Exception as e:
            logger.error(f"**Failed gold transformation: {e}**")
            raise

    