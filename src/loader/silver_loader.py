import pandas as pd
from sqlalchemy import text
from utils.logger import get_logger
import utils.db as db

logger = get_logger(__name__)

class SilverLoader:
    def __init__(self, normalized_silver_schema):
        """
        engine: Đối tượng engine từ hàm get_sqlalchemy_engine() của bạn
        """
        self.engine = db.get_sqlalchemy_engine()
        self.normalized_silver_schema = normalized_silver_schema
        self.logger = logger

    def load_table(self, key : str, df : pd.DataFrame):
        try:
            row_count = 0 if df is None else len(df)
            self.logger.info(f"Loading {row_count} rows into silver.{key}...")

            # Convert pandas NA (pd.NA / <NA>) to None so SQL driver can handle NULLs
            df_to_write = df.copy() if df is not None else pd.DataFrame()
            df_to_write = df_to_write.where(pd.notnull(df_to_write), None)

            # Insert in chunks and commit each chunk separately to avoid a single large
            # transaction that may be aborted mid-run (which would leave 0 rows).
            chunksize = 5000
            if row_count == 0:
                return

            for start in range(0, row_count, chunksize):
                end = min(start + chunksize, row_count)
                chunk = df_to_write.iloc[start:end]
                try:
                    chunk.to_sql(
                        name=key,
                        con=self.engine,
                        schema="silver",
                        if_exists="append",
                        index=False,
                        method="multi"
                    )
                    self.logger.info(f"Inserted rows {start}-{end-1} into silver.{key}")
                except Exception as e:
                    self.logger.error(f"Failed inserting rows {start}-{end-1} into silver.{key}: {e}", exc_info=True)
                    raise
        except Exception as e:
            self.logger.error(f"Failed loading table {key}: {e}", exc_info=True)
            raise
    def load_silver(self):        
        try:
            load_order =[
                "broadcast",
                "rating",
                "anime",
                "theme",
                "demographic",
                "genre",
                "organization",
                "anime_theme",
                "anime_organization",
                "anime_demographic",
                "anime_genre"
            ]
            # Truncate target tables first to avoid primary key conflicts / duplicates
            for table in load_order:
                try:
                    stmt = text(f"TRUNCATE TABLE silver.{table} RESTART IDENTITY CASCADE")
                    # Use a begin() context to get a connection and ensure the statement executes
                    with self.engine.begin() as conn:
                        conn.execute(stmt)
                    self.logger.info(f"Truncated table silver.{table} successfully")
                except Exception as e:
                    # If truncate fails for any table, log the exception details and continue
                    self.logger.warning(f"Could not truncate silver.{table}: {e}")

            for table in load_order:
                self.load_table(table, self.normalized_silver_schema[table])
            
        except Exception as e:
            self.logger.error(f"Failed to load data to Silver: {e}", exc_info=True)
            raise