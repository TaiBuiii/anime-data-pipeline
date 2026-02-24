import utils.db as db
from utils.logger import get_logger
from processors.anime_core import process_anime_core
import pandas as pd
 
logger = get_logger(__name__)


def run_transformation() -> None:
    engine = db.get_sqlalchemy_engine() 
    try:
        query = "SELECT payload FROM bronze.anime_raw"
        df_bronze = pd.read_sql(query,engine)
        process_anime_core(df_bronze)
        
        
    except Exception as e:
        logger.error(f"Transformation Error occured{e}:")
