from stage.jikan_ingestor import JikanIngestor
from loader.stage_loader import StageLoader
from utils.logger import get_logger

logger = get_logger(__name__)
class StageOrchestrator:
    """
    Orchestrates the entire stage layer ETL (Ingestion & Loading) pipeline.
    This class handles fetching raw data from the Jikan API (via JikanIngestor)
    and loading it directly into the stage tier database (via StageLoader).
    """
    def __init__(self, db_name = "animedw"):
        """Initializes the components needed for the orchestration.
        
        Args:
            db_name (str): The name of the target database. Defaults to "animedw".
        """
        self.stage_ingestor = JikanIngestor()
        self.stage_loader = StageLoader(db_name)

    def run_stage_ingestion(self):
        """
        Executes the data ingestion loop and loads data into the Stage layer.
        
        Iterates through data of each page yielded by the ingestor, loads them 
        into the database
        """
        logger.info("**Ingesting Stage**")
        try:
            total_anime : int = 0

            # Iterate through DataFrame chunks yielded by the JikanIngestor generator
            for df_anime_raw, df_anime_pagination_log in self.stage_ingestor.run_ingestion():

                # Load the raw data and pagination logs into the stage database
                self.stage_loader.load_stage(df_anime_raw, df_anime_pagination_log)

                # Update and log the total number of anime records processed so far
                total_anime += len(df_anime_raw)
                logger.info(f"Currently loaded {total_anime}")

            logger.info("**Ingesting stage successfully**")
        except Exception as e:
            logger.error(f"**Ingestion Failed{e}**")
            raise
