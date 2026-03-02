from utils.logger import get_logger
from loader.base_loader import BaseLoader

logger = get_logger(__name__)

class SilverLoader(BaseLoader):
    def __init__(self, db_name="animed"):
        super().__init__(db_name = db_name)
    


    def load_silver(self, normalized_silver_schema):        
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

            for table in load_order:
                self.db_manager.load_table(table, "silver" , normalized_silver_schema[table])
            
        except Exception as e:
            logger.error(f"Failed to load data to Silver: {e}", exc_info=True)
            raise