from utils.logger import get_logger
from utils.db import DatabaseManager

logger = get_logger(__name__)

class BaseLoader:
    def __init__(self, db_name : str):
        self.db_manager : DatabaseManager = DatabaseManager(db_name)
 
