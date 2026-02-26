from ingestion.bronze_ingestor.jikan_ingestor import JikanIngestor
from loader.bronze_loader import load_bronze

def run_ingestion():
    ingestor = JikanIngestor()
    for anime_data, pagination_data in ingestor.run_ingestion():
        load_bronze(anime_data, pagination_data,ingestor.conn)