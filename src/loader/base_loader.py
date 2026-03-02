from utils.logger import get_logger
from utils.db import DatabaseManager
import atexit

logger = get_logger(__name__)

class BaseLoader:
    def __init__(self, db_name : str):
        self.db_manager : DatabaseManager = DatabaseManager(db_name)
        atexit.register(self.close)
 

    def close(self):
        if hasattr(self, "db_manager") and self.db_manager:
            try:
                self.db_manager.dispose()
            except Exception as e:
                logger.error(f"Failed closing database connection: {e}")