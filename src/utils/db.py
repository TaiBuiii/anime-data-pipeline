import psycopg2
from dotenv import load_dotenv
import os
from logger import get_logger

logger = get_logger(__name__)
load_dotenv() 


def get_postgres_connection():
    try:

        conn = psycopg2.connect(host = os.getenv("DB_HOST"),
                                user = os.getenv("DB_USER"),
                                password = os.getenv("DB_PASSWORD"),
                                dbname = "postgres",
                                port = int(os.getenv("DB_PORT",5432))) 
        logger.info("Connecting To postgres Database Sucessfully")
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise
    
def get_animedw_connection():
    try:
        conn = psycopg2.connect(host = os.getenv("DB_HOST"),
                                user = os.getenv("DB_USER"),
                                password = os.getenv("DB_PASSWORD"),
                                dbname = "animedw",
                                port = int(os.getenv("DB_PORT",5432))) 
        logger.info("Connecting To animedw Database Sucessfully")
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise
if __name__ == "__main__":
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print(cur.fetchone())
    cur.close()
    conn.close()