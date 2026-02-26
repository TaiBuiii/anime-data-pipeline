import pandas as pd
from utils.logger import get_logger
logger = get_logger(__name__)
class Extractor:
    def __init__(self, df_raw : pd.DataFrame):
        self.logger = logger
        self.logger.info("Initializing Extractor")
        self.df_bronze = pd.json_normalize(df_raw["payload"]).rename(columns={"mal_id":"anime_mal_id"})

    def _extract_anime(self) -> pd.DataFrame:
        self.logger.info("Extracting anime")
        try:
            # Extract information related to anime from the first layer of JSON 
            cols = [
            "anime_mal_id", "title", "title_english", "title_japanese","url",
            "type", "source", "episodes", "duration", "rating", "score",
            "scored_by", "popularity", "favorites", "airing", "status",
            "aired.from", "aired.to", "season", "broadcast.day",
            "broadcast.time", "broadcast.timezone"
            ]
            return self.df_bronze[cols].copy()
        except Exception as e:
            logger.error(f"Failed extracting anime: {e}")
            raise
 
    def _map_metadata_to_anime(self, prefix : str) -> pd.DataFrame:
        self.logger.info(f"Mapping {prefix} metadata to anime")

        # Only take anime's id and column contains specified prefix's data to work, avoiding overloading RAM
        df_working = self.df_bronze[['anime_mal_id', prefix]].copy()
        try:

            # Explode anime that have multiple prefix values into records
            df_working = df_working[["anime_mal_id",prefix]].explode(prefix)

            # Only keeps prefix that have relation to anime
            df_working = df_working.dropna(subset = [prefix])

            # Unnest and store all the data of prefix field to separated dataframe
            df_flat = pd.json_normalize(df_working[prefix])

            # reset index of df_working containing anime_mal_id, and df_flat containing prefix data
            df_working.reset_index(drop=True, inplace= True)
            df_flat.reset_index(drop=True, inplace= True)

            prefix = prefix.rstrip("s")
            
            # Check if prefix is organization
            if prefix in ["producer", "studio", "licensor"]:

                # rename original organization's mal_id to orginzation_mal_id and specify how organization related to anime
                df_flat = df_flat.rename(columns={"mal_id" : "organization_mal_id"}).assign(role = prefix)

                # choose the anime's id, organization's id and the relationship between them to be combined
                concat_list = [df_working["anime_mal_id"],df_flat[["organization_mal_id","role"]]]

                # return the combined dataframe
                return pd.concat(concat_list, axis = 1)
            
            # Else of prefix is other metadata
            else:
                # rename orignal prefix's id to prefix_mal_id
                df_flat = df_flat.rename(columns={"mal_id" : f"{prefix}_mal_id"})

                # choose anime's id and prefix's id to be combined
                concat_list = [df_working["anime_mal_id"],df_flat[f"{prefix}_mal_id"]]

                # return the combined dataframe
                return pd.concat(concat_list, axis = 1)

        except Exception as e:
            self.logger.error(f"Failed mapping metadata to anime: {e}")
            raise
    
    def _extract_metadata(self, prefix : str) -> pd.DataFrame:
        self.logger.info(f"Extracting {prefix} metadata")

        # Only take anime's id and column contains specified prefix's data to work, avoiding overloading RAM
        df_working = self.df_bronze[prefix].copy()
        try:

            # Explode anime that have multiple prefix values into records
            df_working = df_working.explode() 

            # Discard record that have no prefix value
            df_working = df_working.dropna()

            # Extract the data related to prefix
            df_working = pd.json_normalize(df_working)

            # Only keeps distinct prefix's id, prefix's mame, prefix's url
            df_working = df_working[["mal_id","name","url"]].drop_duplicates()
            return df_working.sort_values("mal_id")
        except Exception as e:
            self.logger.error(f"Failed Extracting metadata: {e}")
            raise

    def run_extraction(self) -> dict[pd.DataFrame]:
        self.logger.info("Running extraction")
        try:
            # Extract tables for silver_schema
            return {

                # Extract anime core information
                "df_anime" : self._extract_anime(),

                # Extract metadata
                "df_genre" : self._extract_metadata("genres"),
                "df_theme" : self._extract_metadata("themes"),
                "df_demographic" : self._extract_metadata("demographics"),
                "df_organization" : pd.concat([self._extract_metadata(prefix) for prefix in ["producers", "studios", "licensors"]]),

                # Handle many-to-many relationship
                "df_anime_gerne" : self._map_metadata_to_anime("genres"),
                "df_anime_theme" : self._map_metadata_to_anime( "themes"),
                "df_anime_demographic" : self._map_metadata_to_anime("demographics"),
                "df_anime_organization" : pd.concat([self._map_metadata_to_anime(prefix) for prefix in ["producers", "studios", "licensors"]])
            }
        except Exception as e:
            self.logger.error(f"Failed running extraction: {e}")
            raise