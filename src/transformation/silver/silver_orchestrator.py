import utils.db as db
from utils.logger import get_logger
from transformation.silver.silver_transformer.extractor import extractor
import pandas as pd

logger = get_logger(__name__)
    

def run_transformation():
    engine = db.get_sqlalchemy_engine() 
    try:
        # Read raw json from bronze.anime_raw
        query = "SELECT payload FROM bronze.anime_raw"
        payload = pd.read_sql(query,engine)

        # Extract data from bronze
        silver_schema = extractor(payload).run_extraction()

        # Clean Data

        return silver_schema
    except Exception as e:
        logger.error(f"Transformation Error occured{e}:")
