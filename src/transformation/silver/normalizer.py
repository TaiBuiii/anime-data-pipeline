import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

class Normalizer:
    """
    Handles the relational normalization of data frames moving into the final Silver layer.
    
    This class enforces clean Star Schema database rules by:
    - Splitting repeating attributes into separate dimension tables with auto-incrementing Surrogate Keys.
    - Consolidating uniform organizational structures (producers, studios, licensors) into a single unified schema.
    """
    def __init__(self, cleaned_silver_schema : dict[str, pd.DataFrame]):
        """
        Initializes the Normalizer with pre-cleaned Silver layer data.
        
        Args:
            cleaned_silver_schema (dict[str, pd.DataFrame]): A dictionary mapping schema table names 
                                                             to their respective cleaned DataFrames.
        """
        self.cleaned_silver_schema = cleaned_silver_schema

    @staticmethod
    def _split_table(df_original : pd.DataFrame, id_name : str, on : list[str]) -> pd.DataFrame:
        """Extracts unique combinations of columns to form a separate Dimension lookup table.
        
        Generates an auto-incrementing numerical surrogate key starting from 1 for the dimension entries.
        
        Args:
            df_original (pd.DataFrame): The source DataFrame containing the columns to isolate.
            id_name (str): The name designated for the new surrogate ID column.
            on (list[str]): The list of target columns to extract and deduplicate.
            
        Returns:
            pd.DataFrame: A new isolated lookup Dimension table with a sequential identifier.
        """
        df_split = df_original[on].drop_duplicates().copy()
        df_split.insert(0, id_name, range(1, len(df_split) + 1))
        df_split[id_name] = df_split[id_name].astype("Int64")
        return df_split
    
    @staticmethod
    def _map_original_table(df_original : pd.DataFrame, df_split : pd.DataFrame, on : list[str]) -> pd.DataFrame:
        """Replaces natural descriptive attributes in the main table with their newly generated surrogate ID.
        
        Args:
            df_original (pd.DataFrame): The core central table containing descriptive data rows.
            df_split (pd.DataFrame): The generated lookup dimension table containing the new ID.
            on (list[str]): The intersection columns used to perform the mapping operation.
            
        Returns:
            pd.DataFrame: The main table with descriptive text stripped out and replaced with Foreign Key IDs.
        """
        df_original = df_original.merge(df_split, on = on, how = "left")
        df_original = df_original.drop(columns = on)
        return df_original
    
    @staticmethod
    def _normalize_anime_metadata_relationship(df_original : pd.DataFrame, prefix : str) -> dict[pd.DataFrame]:
        """Deconstructs categorical bridge tables into distinct isolated entity tables and thin relational bridges.
        
        Separates descriptive attributes (name, url) into an independent master dimension, leaving 
        the associative bridge mapped cleanly to the `anime_mal_id`.
        
        Args:
            df_original (pd.DataFrame): The shared junction bridge data containing entity metadata.
            prefix (str): The category name identifier (e.g., 'genre', 'theme', 'organization').
            
        Returns:
            dict[str, pd.DataFrame]: A dict holding the normalized bridge table and the standalone master metadata table.

        """
        logger.info(f"Normalizing anime {prefix} relationship table")
        try:
            df_split = df_original [[f"{prefix}_mal_id","name","url"]].copy()
            df_split = df_split.drop_duplicates()
            df_original = df_original.drop(columns = ["url","type","name"])
            return {
                    f"anime_{prefix}": df_original,
                    f"{prefix}" : df_split
            }
        except Exception as e:
            logger.error(f"**Failed normalizing anime {prefix} relationship table: {e}**", exc_info=True)
            raise

    @staticmethod
    def _combine_organizations(cleaned_silver_schema : dict[str,pd.DataFrame], organizations : list[str]):
        """Combines multiple separate company entities into a unified organizational table using role flags (Producers, Studios, and Licensors).
        
        Args:
            cleaned_silver_schema (dict[str, pd.DataFrame]): Dictionary containing active schema DataFrames.
            organizations (list[str]): Names of the target organization categories to bundle together.
            
        Returns:
            dict[str, pd.DataFrame]: A single dictionary item tracking the integrated 'df_anime_organization' table.
        """
        logger.info("Combining organizations")
        try:
            concate_list = []
            for organization in organizations:
                df_anime_organization = cleaned_silver_schema[f"df_anime_{organization}"].copy()

                # Standardize category explicit IDs to a shared 'organization_mal_id'
                df_anime_organization.rename(columns = {f"{organization}_mal_id":"organization_mal_id"}, inplace=True)
                df_anime_organization["role"] = organization
                concate_list.append(df_anime_organization)
            
            # Flatten components vertically into one shared DataFrame
            return {"df_anime_organization" : pd.concat(concate_list, ignore_index=True)}
        except Exception as e:
            logger.error(f"**Failed combining organizations: {e}**", exc_info=True)
            raise


    def _normalize_anime(self, df_anime : pd.DataFrame) -> dict[pd.DataFrame]:
        """
        Executes complete structural normalization transformations specifically for the Anime main core table.
        
        Deconstructs repetitive inline structures (Broadcast details and Ratings logs) into distinct 
        dimension tables linked via clean surrogate keys.
        
        Args:
            df_anime (pd.DataFrame): The pre-cleaned central Anime master table.
            
        Returns:
            dict[str, pd.DataFrame]: A dictionary containing the thin fact-like Anime table along 
                                    with the newly spawned dimensions.
        """
        logger.info("Normalizing anime table")
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
                "anime" : df_anime,
                "broadcast" : df_broadcast,
                "rating" : df_rating
            }
        except Exception as e:
            logger.error(f"**Failed normalizing anime table: {e}**", exc_info=True)
            raise

    @staticmethod
    def normalize_column_name(normalized_silver_schema: dict[str, pd.DataFrame]):
        """
        Standardizes all columns by replacing object dot notation accessors with valid relational snake_case.
        
        Transforms names such as 'broadcast.timezone' into 'broadcast_timezone' to prevent structural 
        query syntax failures.
        
        Args:
            normalized_silver_schema (dict[str, pd.DataFrame]): Dictionary containing normalized active tables.
            
        Returns:
            dict[str, pd.DataFrame]: Updated schema data containing corrected snake_case structural headings.

        """
        logger.info("Converting all column names to snake_case (replacing '.' with '_')")
        try:
            for df in normalized_silver_schema.values():
                df.columns = [col.replace(".", "_") for col in df.columns]
            return normalized_silver_schema
        except Exception as e:
            logger.error(f"Failed normalizing all column names to snake_case: {e}", exc_info=True)
            raise


    def run_normalization(self) -> dict[str, pd.DataFrame]:
        """
        Orchestrates and drives the entire normalization process sequence for the data pipeline.
        
        Combines isolated entity metrics, executes entity deconstruction, creates 
        bridge tables, and converts overall schema properties into standard database design conventions.
        
        Returns:
            dict[str, pd.DataFrame]: The complete normalized schema layout map ready for Silver target loading.
        """
        logger.info("Running Normalization process")
        try:
            normalized_silver_schema = {}
            
            # Step 1: Standardize organizational elements into a unified table representation
            df_anime_organization = self._combine_organizations(self.cleaned_silver_schema, ["producer", "studio", "licensor"])
            self.cleaned_silver_schema.update(df_anime_organization)

            # Step 2: Separate multi-attribute inside the main Anime structure
            if "df_anime" in self.cleaned_silver_schema:
                result = self._normalize_anime(self.cleaned_silver_schema["df_anime"])
                normalized_silver_schema.update(result)
            
            # Step 3: Normalize relational many-to-many bridge frameworks
            for table_name in ["df_anime_theme","df_anime_demographic","df_anime_genre", "df_anime_organization"]:
                prefix = table_name.replace("df_anime_", "")  
                result = self._normalize_anime_metadata_relationship(self.cleaned_silver_schema[table_name], prefix)
                normalized_silver_schema.update(result)

            # Step 4: Convert nested structural attributes (dot notations) safely into snake_case headings
            normalized_silver_schema = self.normalize_column_name(normalized_silver_schema)

            logger.info(f"**Normalizing successfully**")
            return normalized_silver_schema
        except Exception as e:
            logger.error(f"**Failed running normalization: {e}**", exc_info=True)
            raise