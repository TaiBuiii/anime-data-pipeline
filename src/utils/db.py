from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import os
from utils.logger import get_logger
import pandas as pd

# create logger object
logger = get_logger(__name__)
# read environment variables from .env file
load_dotenv() 

class DatabaseManager:
    def __init__(self, dbname = "postgres"):
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.dbname = dbname or os.getenv("DB_NAME", "postgres")
        self.port = int(os.getenv("DB_PORT",5432))

        self.connection_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        try:
            self._engine = create_engine(self.connection_url)
            logger.info (f"Successfully initialized SQLAlchemy Engine for database: {self.dbname}")
        except Exception as e:
            logger.error(f"Failed initializing SQLAlchemy Engine for database: {self.dbname} {self.dbname}: {e}")
            raise

    def get_engine(self):
        return self._engine
    

    def query_dataframe(self, query : str, params : dict = None) -> pd.DataFrame:
        try:
            return pd.read_sql(text(query), self._engine, params=params or {})
        except Exception as e:
            logger.error(f"Fail executing query {query}: {e}", exc_info = True)

    
    def execute(self, query : str, params : list[dict] = None):
        try:
            with self._engine.begin() as conn:
                conn.execute(text(query), params or {})
                 
        except Exception as e:
            logger.error(f"Failed Executing {query}")
            raise
    

    def execute_file(self, file: Path, autocommit: bool = False):
        logger.info(f"Executing file {file.name}")
        try:
            with open(file, "r") as f:
                sql_content = f.read()

            statements = [s.strip() for s in sql_content.split(";") if s.strip()]
            if autocommit:
                with self._engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    for statement in statements:
                        conn.execute(text(statement))
            else:
                with self._engine.begin() as conn:
                    for statement in statements:
                        conn.execute(text(statement))
        except Exception as e:
            logger.error(f"Failed executing {file.name}", exc_info=True)
            raise


    def load_table(self, table_name : str, schema : str, df : pd.DataFrame):
        try:
            df.to_sql(
                name=table_name,
                con=self._engine,
                schema= schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000
            )
        except Exception as e:
            logger.error(f"Failed loading table {table_name}: {e}", exc_info=True)
            raise


    def dispose(self):
        try:
            if self._engine:
                self._engine.dispose()
            logger.info("Database engine pool disposed")
        except Exception as e:
            logger.info(f"Failed disposing engine: {e}")

