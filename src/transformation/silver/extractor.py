import utils.db as db
from utils.logger import get_logger
import pandas as pd
 
logger = get_logger(__name__)

def extract_anime(df_raw : pd.DataFrame) -> pd.DataFrame:
    try:
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
        ]]
        return df_anime
    except Exception as e:
        logger.error(f"Failed extracting anime: {e}")
    

def extract_metadata(df_raw : pd.DataFrame, prefix : str):
    df_working = df_raw[prefix].copy()
    try:
        df_working = df_working.explode() 
        df_working = df_working.dropna()
        df_working = pd.json_normalize(df_working)
        df_working = df_working[["mal_id","name","url"]].drop_duplicates()
        return df_working.sort_values("mal_id")
    
    except Exception as e:
        logger.error(f"Error extracting {prefix} metadata: {e}")
    
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

        df_flat = df_flat.rename(columns={"mal_id" : f"{prefix}_mal_id"})
        concat_list = [df_working["anime_mal_id"],df_flat[f"{prefix}_mal_id"]]
        return pd.concat(concat_list, axis = 1)

    except Exception as e:
        logger.error(f"Failed mapping {prefix} metadata to anime: {e}")


def extract_organization_metadata(df_raw : pd.DataFrame):
    try:
        concat_list = [extract_metadata(df_raw, prefix) for prefix in ["producers", "studios", "licensors"]]
        return pd.concat(concat_list)
    except Exception as e:
        logger.error(f"Failed extracting organization metadata: {e}")

def map_organization_metadata_to_anime(df_raw : pd.DataFrame) -> pd.DataFrame:
    try:
        concat_list = [map_metadata_to_anime(df_raw, prefix) for prefix in ["producers", "studios", "licensors"]]
        return pd.concat(concat_list)
    except Exception as e:
        logger.error(f"Failed mapping organization metadata to anime: {e}")