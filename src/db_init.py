"""Database initialization module for anime data pipeline.

This module handles the execution of DDL (Data Definition Language) scripts
to initialize the database schema and structure.
"""
from pathlib import Path
import psycopg2
import utils.db as db
from utils.logger import get_logger

# Path to DDL scripts directory
DDL_PATH = Path(__file__).resolve().parent.parent / "ddl"
logger = get_logger(__name__)


def execute_sql_file(file: Path, conn: psycopg2.connection) -> None:
    """Execute SQL statements from a file.
    
    Reads SQL statements from the given file and executes them one by one
    against the provided database connection. Statements are split by semicolon
    to support files with multiple DDL commands.
    
    Args:
        file: Path to the SQL file to execute
        conn: PostgreSQL connection object with autocommit enabled
        
    Raises:
        Exception: If any SQL statement fails to execute
    """
    try:
        logger.info(f"Executing {file.name}")

        with conn.cursor() as cursor:
            with open(file, "r") as f:
                sql = f.read()
            
            # Split and execute each statement separately
            statements = sql.split(";")
            for statement in statements:
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)

    except Exception as e:
        logger.error(f"Error executing {file.name}: {e}")
        raise


def run_ddl():
    """Initialize the database by executing all DDL scripts.
    
    This function performs the following steps:
    1. Connects to the default 'postgres' database with autocommit enabled
    2. Executes the database creation script (01_create_database.sql)
    3. Connects to the newly created 'animedw' database with autocommit enabled
    4. Executes all remaining DDL scripts to create schemas and tables
    
    """
    
    # Step 1: Connect to default postgres database and create animedw
    conn = db.get_postgres_connection()
    conn.autocommit = True
    execute_sql_file(DDL_PATH / "01_create_database.sql", conn)
    conn.close()

    # Step 2: Connect to animedw and execute remaining DDL scripts
    conn = db.get_animedw_connection()
    conn.autocommit = True
    for file in sorted(DDL_PATH.iterdir()):
        if file.name != "01_create_database.sql":
            execute_sql_file(file, conn)
    conn.close()
        

