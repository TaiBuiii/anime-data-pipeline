from utils.logger import get_logger
from loader.base_loader import BaseLoader

logger = get_logger(__name__)

class EdwLoader(BaseLoader):
    """Handles the sequential loading of normalized DataFrames into the edw layer database.
    
    Inherits from BaseLoader to leverage shared database session states. This loader 
    maintains a strict execution order to respect foreign key dependencies and database 
    integrity constraints.
    """
    def __init__(self, db_name = "animedw"):
        """Initializes the EdwLoader with a database connection and table sequence configurations.
        
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
    
    def load_edw(self, normalized_edw_schema):        
        try:
            for table in self.load_order:
                self.db_manager.load_table(table, "edw" , normalized_edw_schema[table])
            
        except Exception as e:
            logger.error(f"Failed to load data to edw: {e}", exc_info=True)
            raise