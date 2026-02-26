import psycopg2 
from psycopg2.extras import Json
from psycopg2.extras import execute_values
from loader.base_loader import BaseLoader
from utils.logger import get_logger
import utils.db as db 

class BronzeLoader(BaseLoader):
    def _insert_anime_raw(self, anime_raw : list[tuple], cursor):
        """
        This function is called by load_bronze() to execute sql query, inserting
        data into bronze.anime_raw
        """

        # Inserting data to bronze.anime_raw
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
                anime_raw)
            
        except Exception as e:
            self.logger.error(f"Cannot load bronze.anime_raw: {e}")
            raise
            
    def _insert_anime_pagination_log(self, anime_pagination_log : list[tuple], cursor):
        """
        This function is called by load_bronze() to execute sql query, inserting
        data into bronze.anime_pagination_log
        """
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
            self.logger.error(f"Cannot load bronze.anime_pagination_log: {e}")
            raise

        
    def load_bronze(self, anime_raw : list[tuple], anime_pagination_log : list[tuple]):
        """
        This function acts as the orchestrator which calls insert_anime_raw() 
        and insert_anime_pagination_log() to perform insertion

        """
        try:
            with self.conn.cursor() as cursor:
                self._insert_anime_raw(anime_raw, cursor)
                self._insert_anime_pagination_log(anime_pagination_log, cursor)

            self.conn.commit()  
            self.logger.info(f"Load Successfully {len(anime_raw)}")

        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            raise

