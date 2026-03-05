import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

class Extractor:
    def __init__(self, payload : pd.DataFrame):
        self.df_bronze = pd.json_normalize(payload["payload"]).rename(columns={"mal_id":"anime_mal_id"})


    def _extract_nested_metadata(self, prefix : str) -> pd.DataFrame:
        logger.info(f"Mapping {prefix} metadata to anime")

        # Only take anime's id and column contains specified prefix's data to work, avoiding overloading RAM
        df_working = self.df_bronze[['anime_mal_id', prefix]].copy()
        try:

            # Explode anime that have multiple prefix values into records
            df_working = df_working.explode(prefix).dropna(subset = [prefix])


            # Unnest and store all the data of prefix field to separated dataframe
            df_flat = pd.json_normalize(df_working[prefix])

            prefix = prefix.rstrip("s")
        
            # rename orignal prefix's id to prefix_mal_id
            df_flat = df_flat.rename(columns={"mal_id" : f"{prefix}_mal_id"})

            # choose anime's id and prefix's information to be combined
            concat_list = [df_working['anime_mal_id'], df_flat]

            # return the combined dataframe
            return pd.concat(concat_list, axis = 1)

        except Exception as e:
            logger.error(f"**Failed mapping metadata to anime: {e}**", exc_info=True)
            raise
    

    def run_extraction(self) -> dict[str,pd.DataFrame]:
        logger.info("Running extraction")
        try:
            # Extract tables for silver_schema
            silver_schema =  {

                # Extract anime core information
                "df_anime" : self.df_bronze,

                # Handle many-to-many relationship
                "df_anime_genre" : self._extract_nested_metadata("genres"),
                "df_anime_theme" : self._extract_nested_metadata( "themes"),
                "df_anime_demographic" : self._extract_nested_metadata("demographics"),
                "df_anime_producer" : self._extract_nested_metadata("producers"),
                "df_anime_studio" : self._extract_nested_metadata("studios"),
                "df_anime_licensor" : self._extract_nested_metadata("licensors")

            }
            logger.info("**Extracting Successfully**")
            return silver_schema

        except Exception as e:
            logger.error(f"**Failed running extraction: {e}**", exc_info=True)
            raise