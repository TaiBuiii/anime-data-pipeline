import psycopg2 
from psycopg2.extras import Json
from psycopg2.extras import execute_values
from utils.logger import get_logger
import utils.db as db 

logger = get_logger(__name__)

def insert_anime_raw(anime_raw, cursor):
    formatted = [
        (mal_id, page, Json(payload))
        for mal_id, page, payload in anime_raw
    ]
    
    try:
        execute_values(
            cursor,
            """
            INSERT INTO bronze.anime_raw
            (
            mal_id,
            page, 
            payload
            ) VALUES %s
            ON CONFLICT (mal_id, page) DO NOTHING
            """,
            formatted)
        
    except Exception as e:
        logger.error(f"Cannot load bronze.anime_raw: {e}")
        raise
            
def insert_anime_pagination_log(anime_pagination_log, cursor):
        try:
            execute_values(
                cursor,
            """
            INSERT INTO bronze.anime_pagination_log
            ( 
            page ,
            last_visible_page ,
            has_next_page ,
            items_count ,
            items_total ,
            items_per_page
            ) VALUES %s
            ON CONFLICT (page) DO NOTHING
            """,
            anime_pagination_log)

        except Exception as e:
            logger.error(f"Cannot load bronze.anime_pagination_log: {e}")
            raise

        
def load_bronze(anime_raw, anime_pagination_log, conn):
    try:
        with conn:
            with conn.cursor() as cursor:
                insert_anime_raw(anime_raw, cursor)
                insert_anime_pagination_log(anime_pagination_log, cursor)
        logger.info(f"Load Successfully {len(anime_raw)}")

    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise

