
from pathlib import Path
import psycopg2
from utils.db import DatabaseManager
from utils.logger import get_logger

logger = get_logger(__name__)
class DatabaseInitializer:
    def __init__(self):
        self.DDL_PATH = Path(__file__).resolve().parent.parent / "ddl"
        self.postgres_manager = DatabaseManager("postgres")
        self.animedw_manager = DatabaseManager("animedw")


    def _create_database(self):
        logger.info("Creating database animedw")
        try:
            self.postgres_manager.execute_file(self.DDL_PATH / "01_create_database.sql", autocommit=True)
        except Exception as e:
            logger.error(f"Failed creating database: {e}", exc_info=True)
            raise


    def _create_schemas(self):
        try:
            for file in sorted(self.DDL_PATH.iterdir()):
                if file.name != "01_create_database.sql":
                    self.animedw_manager.execute_file(file)
        except Exception as e:
            logger.error(f"Failed creating schemas: {e}", exc_info=True)
            raise

    
    def run_ddl(self):
        # Step 1: Connect to default postgres database and create animedw
        self._create_database()

        # Step 2: Connect to animedw and execute remaining DDL scripts
        self._create_schemas()


        

