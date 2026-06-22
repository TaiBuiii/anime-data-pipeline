from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import os
from utils.logger import get_logger
import pandas as pd
import atexit

# create logger object
logger = get_logger(__name__)

# read environment variables from .env file
load_dotenv() 

class DatabaseManager:
    """
    This class is responsible for managing all the connection and executing SQL query using SQLAlchemy 
    """
    def __init__(self, dbname = "postgres"):

        # initialize object with the loaded configurations from .env
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.dbname = dbname or os.getenv("DB_NAME", "postgres")
        self.port = int(os.getenv("DB_PORT",5432))

        # create connection URL to connect PostgreSQL 
        self.connection_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

        try:

            # create engine, maintaining  connection pool
            self._engine = create_engine(self.connection_url)
            logger.info (f"Successfully initialized SQLAlchemy Engine for database: {self.dbname}")
        except Exception as e:
            logger.error(f"Failed initializing SQLAlchemy Engine for database: {self.dbname} {self.dbname}: {e}")
            raise

        # dispose when finishing 
        atexit.register(self.dispose)

    def get_engine(self):
        """
        return _engine object
        """
        return self._engine
    

    def query_dataframe(self, query : str, params : dict = None) -> pd.DataFrame:
        """
        Performing a SQL query and returning the result as a DataFrame 
        """
        try:
            # accepts a SQL query and return the result 
            return pd.read_sql(text(query), self._engine, params=params or {})
        except Exception as e:
            logger.error(f"Fail executing query {query}: {e}", exc_info = True)

    
    def execute(self, query : str, params : list[dict] = None):
        """
        Execute DML such as (INSERT, UPDATE, DELETE) in a transaction. If error occurs, the system automatically rollbacks to maintain data integrity.
        """
        try:
            # open a transaction
            with self._engine.begin() as conn:

                # execute the commands. Only COMMIT when successfully
                conn.execute(text(query), params or {})
                 
        except Exception as e:
            logger.error(f"Failed Executing {query}")
            raise
    

    def execute_file(self, file: Path, autocommit: bool = False):
        """
        Execute SQL files
        """
        logger.info(f"Executing file {file.name}")
        try:
            
            # open SQL file
            with open(file, "r") as f:
                
                # read the content from the file
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
        """
        Performs bulk loading from a Pandas DataFrame into a database table.
        This technique optimizes write speed and minimizes I/O overhead on the database server.
        """
        try:
            df.to_sql(
                name=table_name,
                con=self._engine,
                schema= schema,
                if_exists="append", # Appends new data to the existing table without losing old data
                index=False, # Avoids saving the Pandas DataFrame index as an SQL column
                method="multi", # Groups multiple records into a single INSERT statement instead of running row by row
                chunksize=5000 # Batches data in chunks
            )
        except Exception as e:
            logger.error(f"Failed loading table {table_name}: {e}", exc_info=True)
            raise


    def dispose(self):
        """
        Closes and completely disposes of the Connection Pool.
        Releases application server resources and shuts down idle connections on the database server.
        """
        try:
            if self._engine:
                self._engine.dispose()
            logger.info("Database engine pool disposed")
        except Exception as e:
            logger.error(f"Failed disposing engine: {e}", exc_info=True)
            raise

