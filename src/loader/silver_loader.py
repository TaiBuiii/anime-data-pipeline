from utils.logger import get_logger
from loader.base_loader import BaseLoader

logger = get_logger(__name__)

class SilverLoader(BaseLoader):
    """Handles the sequential loading of normalized DataFrames into the Silver layer database.
    
    Inherits from BaseLoader to leverage shared database session states. This loader 
    maintains a strict execution order to respect foreign key dependencies and database 
    integrity constraints.
    """
    def __init__(self, db_name = "animedw"):
        """Initializes the SilverLoader with a database connection and table sequence configurations.
        
        Args:
            db_name (str): The name of the target relational database warehouse. Defaults to "animedw".
        """
        super().__init__(db_name = db_name)
        self.load_order = [
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
    
    def load_silver(self, normalized_silver_schema):        
        try:
            for table in self.load_order:
                self.db_manager.load_table(table, "silver" , normalized_silver_schema[table])
            
        except Exception as e:
            logger.error(f"Failed to load data to Silver: {e}", exc_info=True)
            raise