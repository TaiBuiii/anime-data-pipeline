import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

class Normalizer:
    def __init__(self, cleaned_silver_schema : dict[str, pd.DataFrame]):
        self.cleaned_silver_schema = cleaned_silver_schema
        self.logger = logger

    @staticmethod
    def _split_table(df_original : pd.DataFrame, id_name : str, on : list[str]) -> pd.DataFrame:
        df_split = df_original[on].drop_duplicates().copy()
        df_split.insert(0, id_name, range(1, len(df_split) + 1))
        df_split[id_name] = df_split[id_name].astype("Int64")
        return df_split
    
    @staticmethod
    def _map_original_table(df_original : pd.DataFrame, df_split : pd.DataFrame, on : list[str]) -> pd.DataFrame:
        df_original = df_original.merge(df_split, on = on, how = "left")
        df_original = df_original.drop(columns = on)
        return df_original
    
    @staticmethod
    def _normalize_anime_metadata_relationship(df_original, prefix : str) -> dict[pd.DataFrame]:
        logger.info(f"Normalizing anime {prefix} relationship table")
        try:
            df_split = df_original [[f"{prefix}_mal_id","name","url"]].copy()
            df_split = df_split.drop_duplicates()
            df_original = df_original.drop(columns = ["url","type","name"])
            return {
                    f"df_anime_{prefix}": df_original,
                    f"df_{prefix}" : df_split
            }
        except Exception as e:
            logger.error(f"**Failed normalizing anime {prefix} relationship table**")
            raise

    @staticmethod
    def _combine_organizations(cleaned_silver_schema : dict[str,pd.DataFrame], organizations : list[str]):
        logger.info("Combining organizations")
        try:
            concate_list = []
            for organization in organizations:
                df_anime_organization = cleaned_silver_schema[f"df_anime_{organization}"].copy()
                df_anime_organization.rename(columns = {f"{organization}_mal_id":"organization_mal_id"}, inplace=True)
                df_anime_organization["role"] = organization
                concate_list.append(df_anime_organization)

            return {"df_anime_organization" : pd.concat(concate_list, ignore_index=True)}
        except Exception as e:
            logger.error(f"**Failed combining organizations: {e}**")
            raise


    def _normalize_anime(self, df_anime : pd.DataFrame) -> dict[pd.DataFrame]:
        self.logger.info("Normalizing anime table")
        try: 
            # split Broadcast
            cols_bc = ["broadcast.day", "broadcast.time", "broadcast.timezone"]
            df_broadcast = self._split_table(df_anime, "broadcast_id", cols_bc)
            df_anime = self._map_original_table(df_anime, df_broadcast, cols_bc)

            # split Rating 
            cols_rt = ["rating_code", "rating_description"]
            df_rating = self._split_table(df_anime, "rating_id", cols_rt)
            df_anime = self._map_original_table(df_anime, df_rating, cols_rt)
            
            return {
                "df_anime" : df_anime,
                "df_broadcast" : df_broadcast,
                "df_rating" : df_rating
            }
        except Exception as e:
            self.logger.error(f"**Failed normalizing anime table: {e}**")
            raise


    def run_normalization(self) -> dict[str, pd.DataFrame]:
        self.logger.info("Running Normalization process")
        try:
            normalized_silver_schema = {}
            
            df_anime_organization = self._combine_organizations(self.cleaned_silver_schema, ["producer", "studio", "licensor"])
            self.cleaned_silver_schema.update(df_anime_organization)

            if "df_anime" in self.cleaned_silver_schema:
                result = self._normalize_anime(self.cleaned_silver_schema["df_anime"])
                normalized_silver_schema.update(result)
            
            for table_name in ["df_anime_theme","df_anime_demographic","df_anime_genre", "df_anime_organization"]:
                prefix = table_name.replace("df_anime_", "")  
                result = self._normalize_anime_metadata_relationship(self.cleaned_silver_schema[table_name], prefix)
                normalized_silver_schema.update(result)


            self.logger.info(f"**Normalizing successfully**")
            return normalized_silver_schema
        except Exception as e:
            self.logger.error(f"**Failed running normalization: {e}**")
            raise