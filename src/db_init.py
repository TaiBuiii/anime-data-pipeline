from pathlib import Path
import psycopg2
import logging
import utils.db as db
from utils.logger import get_logger

DDL_PATH = Path(__file__).resolve().parent.parent / "ddl"
logger = get_logger(__name__)


def execute_sql_file(file : Path, conn : psycopg2.connection) -> None:
    """
    execute_sql_file() is responsible for executing sql files
    """
    try:
        logger.info(f"Executing {file.name}")
        with conn.cursor() as cursor:
            with open(file, 'r') as f:
                sql = f.read()
            cursor.execute(sql)
            logger.info(f"Executed {file.name} successfully")
                
    except Exception as e:
        logger.error(f"Error occured: {e}")
        conn.rollback()


def create_database() -> None:
    """
    create_database() reads sql files to create animedw database.
    """
    # connect to default postgres database
    conn = db.get_postgres_connection()
    conn.autocommit = True  
    try:
        # drop animedw if exists, and re-create amimedw
        execute_sql_file(DDL_PATH / "01_drop_database.sql", conn)
        execute_sql_file(DDL_PATH / "02_create_database.sql", conn)
    
    except Exception as e:
        # catch error
        logger.error(f"Cannot create animedw: {e}")
        conn.rollback()

    finally:
        # close connection to postgres database
        conn.close()


def create_schema() -> None:
    """
    create_schema() reads sql files to create bronze, silver, gold schemas
    """
    # connect to newly created animewh
    conn = db.get_animedw_connection()
    conn.autocommit = True
    try:
        # creates schemas
        execute_sql_file (DDL_PATH / "03_create_schema.sql", conn)

    except Exception as e:
        # catch error
        logger.error(f"Cannot create schemas: {e}")
        
    finally:
        # close connection to animedw
        conn.close()


def run_ddl():
    create_database()
    create_schema()