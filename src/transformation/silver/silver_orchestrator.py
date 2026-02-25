import utils.db as db
from utils.logger import get_logger
import pandas as pd
 
logger = get_logger(__name__)

def process_anime(df_raw : pd.DataFrame) -> pd.DataFrame:
    df_anime = df_raw[[
    "mal_id",
    "title",
    "title_english",
    "title_japanese",
    "url",
    "type",
    "source",
    "episodes",
    "duration",
    "rating",
    "score",
    "scored_by",
    "popularity",
    "favorites",
    "airing",
    "status",
    "aired.from",
    "aired.to",
    "season",
    "broadcast.day",
    "broadcast.time",
    "broadcast.timezone"
    ]].set_index("mal_id")
    return df_anime

    
def map_metadata_to_anime(df_raw : pd.DataFrame, prefix : str) -> pd.DataFrame:
    df_working = df_raw[['anime_mal_id', prefix]].copy()
    try:
        
        df_working = df_working[["anime_mal_id",prefix]].explode(prefix)
        df_working = df_working.dropna(subset = [prefix])
        df_flat = pd.json_normalize(df_working[prefix])

        df_working.reset_index(drop=True, inplace= True)
        df_flat.reset_index(drop=True, inplace= True)

        prefix = prefix.rstrip("s")
        
        if prefix in ["producer", "studio", "licensor"]:

            df_flat = df_flat.rename(columns={"mal_id" : "organization_mal_id"}).assign(role = prefix)
            concat_list = [df_working["anime_mal_id"],df_flat[["organization_mal_id","role"]]]
            return pd.concat(concat_list, axis = 1)

        else:

            df_flat = df_flat.rename(columns={"mal_id" : f"{prefix}_mal_id"})
            concat_list = [df_working["anime_mal_id"],df_flat[f"{prefix}_mal_id"]]
            return pd.concat(concat_list, axis = 1)

    except Exception as e:
        logger.error(f"Error Occurelí {e}")


def map_organization_metadata_to_anime(df_raw : pd.DataFrame) -> pd.DataFrame:
    try:
        df_anime_producer = map_metadata_to_anime(df_raw, "producers")
        df_anime_studio = map_metadata_to_anime(df_raw, "studios")
        df_anime_licensor = map_metadata_to_anime(df_raw, "licensors")
        concat_list = [df_anime_licensor,df_anime_producer,df_anime_studio]
        return pd.concat(concat_list)
    
    except Exception as e:
        logger.error(f"Failed mapping organization metadata to anime: {e}")



def run_transformation() -> None:
    engine = db.get_sqlalchemy_engine() 
    try:
        # Read raw json from bronze.anime_raw
        query = "SELECT payload FROM bronze.anime_raw"
        payload = pd.read_sql(query,engine)

        # flatten json into fields
        df_bronze = pd.json_normalize(payload["payload"])
        df_bronze.rename(columns = {"mal_id":"anime_mal_id"}, inplace= True)
        df_anime = process_anime(df_bronze)
        df_anime_gerne = map_metadata_to_anime(df_bronze, "genres")
        df_anime_theme = map_metadata_to_anime(df_bronze, "themes")
        df_anime_demographic = map_metadata_to_anime(df_bronze, "demographics")
        df_anime_organization = map_organization_metadata_to_anime(df_bronze)

        
        
    except Exception as e:
        logger.error(f"Transformation Error occured{e}:")
