from utils.logger import get_logger
from psycopg2.extensions import connection
import utils.db as db
import atexit

class BaseLoader:
    def __init__(self):
        self.conn = db.get_animedw_connection()
        self.logger = get_logger(self.__class__.__name__)
        atexit.register(self.conn)
    

    def close(self):
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                self.logger.info("Database connection closed")
                self.conn = None
        except Exception as e:
            self.logger.error(f"Failed closing database connection: {e}")