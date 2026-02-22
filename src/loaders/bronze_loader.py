import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from utils.logger import get_logger
import utils.db as db

logger = get_logger(__name__)

def load_bronze(anime_raw: list[tuple], pagination: list[tuple]) -> None:
    if not anime_raw:
        logger.warning("No anime data to insert.")
        return

    conn = None

    try:
        conn = db.get_animedw_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        # ==========================
        # Prepare anime_raw
        # ==========================
        # Convert payload dict -> Json adapter
        anime_values = [
            (mal_id, page, Json(payload))
            for mal_id, page, payload in anime_raw
        ]

        psycopg2.extras.execute_values(
            cursor,
            """
            INSERT INTO bronze.anime_raw (mal_id, page, payload)
            VALUES %s
            ON CONFLICT (mal_id, page) DO NOTHING
            """,
            anime_values,
            page_size=100
        )

        # ==========================
        # Insert pagination
        # ==========================
        if pagination:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO bronze.anime_pagination_log (
                    page,
                    last_visible_page,
                    has_next_page,
                    items_count,
                    items_total,
                    items_per_page
                )
                VALUES %s
                ON CONFLICT (page) DO NOTHING
                """,
                pagination
            )

        conn.commit()
        logger.info(f"Inserted {len(anime_raw)} rows into bronze.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Error loading bronze.")
        raise

    finally:
        if conn:
            cursor.close()
            conn.close()