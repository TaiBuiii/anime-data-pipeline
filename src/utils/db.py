import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from utils.logger import get_logger

# create logger object
logger = get_logger(__name__)
# read environment variables from .env file
load_dotenv() 


def get_postgres_connection():
    """
    return connection object to postgres default database
    """
    # try connecting to postgres default database
    try:
        conn = psycopg2.connect(host = os.getenv("DB_HOST"),
                                user = os.getenv("DB_USER"),
                                password = os.getenv("DB_PASSWORD"),
                                dbname = "postgres",
                                port = int(os.getenv("DB_PORT",5432))) 
        logger.info("Connecting To postgres Database Sucessfully")
        return conn
    except Exception as e:
    # inform if connection fails
        logger.error(f"Connection failed: {e}")
        raise
    
def get_animedw_connection():
    """
    return connection object to animedw database
    """
    # try connecting to animedw database
    try:
        conn = psycopg2.connect(host = os.getenv("DB_HOST"),
                                user = os.getenv("DB_USER"),
                                password = os.getenv("DB_PASSWORD"),
                                dbname = "animedw",
                                port = int(os.getenv("DB_PORT",5432))) 
        logger.info("Connecting To animedw Database Sucessfully")
        return conn
    except Exception as e:
    # inform if connection fails
        logger.error(f"Connection failed: {e}")
        raise

def get_sqlalchemy_engine():
    try:
        host = os.getenv("DB_HOST")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        dbname = "animedw"
        port = int(os.getenv("DB_PORT",5432))
        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(conn_str)
        logger.info ("Connecting to animedw Database using Alchemy successfully")
        return engine
    except Exception as e:
        logger.error(f"Error occured: {e}")
        raise
        

