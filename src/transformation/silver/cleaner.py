import pandas as pd
from utils.logger import get_logger
import re 

logger = get_logger(__name__)

class Cleaner:
    """
    Performs data cleaning, type casting, and Data Quality enforcement.
    
    This class is responsible for refining the Silver layer data by parsing complex 
    strings, normalizing duration strings into minutes, enforcing explicit Pandas 
    nullable data types, and correcting business logic contradictions for both 
    Anime records and Metadata lookups.
    """
    def __init__(self, silver_schema: dict[pd.DataFrame]):
        """
        Initializes the Cleaner with the Silver schema and casting rules.
        
        Args:
            silver_schema (dict[str, pd.DataFrame]): A dictionary mapping table names 
                                                     to their respective Silver DataFrames.
        """
        self.silver_schema = silver_schema
        self.logger = logger

        # Defines target columns belonging to each explicit data type category
        self.casting_map = {
                "int": ["anime_mal_id", "episodes", "scored_by", "popularity", "favorites"],
                "float" : ["score", "duration_per_ep"],
                "string": ["title", "title_english", "title_japanese", "url", "broadcast.timezone","broadcast.day", "type", "source", "status", "season", "rating_code","rating_description"],
                "boolean" : ["airing"],
                "datetime" : ["aired.from","aired.to"],
                "time" : ["broadcast.time"]
            }

    @staticmethod
    def _convert_duration_to_minute(series : pd.Series) -> pd.Series:
        """Converts human-readable duration strings (e.g., '1 hr 20 min', '24 min') into float minutes.
        
        Uses a regular expression to capture hours, minutes, and seconds independently,
        then mathematically convert them into minute value.
        
        Args:
            series (pd.Series): A Pandas Series containing raw duration text.
            
        Returns:
            pd.Series: A float-based Pandas Series representing total minutes.
        """
        logger.info("Converting duration to minute")
        try:
            pattern = r'(?:(\d+)\s*hr)?\s*(?:(\d+)\s*min)?\s*(?:(\d+)\s*sec)?'
            extracted = series.str.extract(pattern)

            hours = pd.to_numeric(extracted[0], errors="coerce").fillna(0)
            minutes = pd.to_numeric(extracted[1], errors="coerce").fillna(0)
            seconds = pd.to_numeric(extracted[2], errors="coerce").fillna(0)

            return hours*60 + minutes + seconds/60
        
        except Exception as e:
            logger.error(f"**Faile converting duration to minute: {e}**",exc_info=True)
            raise

    @staticmethod
    def _split_rating(series : pd.Series) -> pd.DataFrame:
        """
        Splits an age rating string into a distinct rating code and rating description.
        
        Example: "PG-13 - Teens 13 or older" becomes Code: "PG-13", Description: "Teens 13 or older".
        
        Args:
            series (pd.Series): A Pandas Series containing the raw composite rating string.
            
        Returns:
            pd.DataFrame: A DataFrame with two expanded columns representing code and description.
        """
        logger.info("Spliting rating")
        try:

            # Split string at the first occurrence of the custom delimiter
            rating_splits = series.str.split(" - ", n=1, expand = True)
            return rating_splits
        except Exception as e:
            logger.error(f"**Failed Spliting rating metadata: {e}**",exc_info=True)
            raise

    @staticmethod       
    def _to_string(s : pd.Series) -> pd.Series: 
        """Casts a series to Pandas string type, strips trailing whitespaces, and lowercases text."""
        return s.astype("string").str.strip().str.lower()
    
    @staticmethod
    def _to_numeric(s : pd.Series, dtype='Int64') -> pd.Series: 
        """Casts a series to a nullable numeric Pandas type (e.g., Int64, Float64)."""
        return s.astype(dtype)

    @staticmethod
    def _to_boolean(s: pd.Series) -> pd.Series:
        """Normalizes boolean-like representations into a native Pandas Boolean array."""
        return pd.to_numeric(s.replace({'true': "1", 'false': "0"}).fillna("0")).astype("boolean")
        
    @staticmethod
    def _to_datetime(s : pd.Series, format=None) -> pd.Series: 
        """Converts strings to datetimes, coercing parsing errors into NaT values."""
        return pd.to_datetime(s, format=format, errors='coerce')

    def _parsing_anime(self, df : pd.DataFrame) -> pd.DataFrame:
        """
        Handles structural column extractions and transformations on the Anime table.
        
        Deconstructs rating components and converts duration string representations into numerical minutes.
        """
        # Split rating into rating code and rating description
        df[['rating_code','rating_description']] = self._split_rating(df["rating"])
        df.drop(columns = ['rating'], inplace = True)

        # Convert duration to minutes
        df["duration_per_ep"] = self._convert_duration_to_minute(df["duration"])
        df.drop(columns = ["duration"], inplace = True)

        return df

    def _casting_anime(self, df) -> pd.DataFrame:
        """Sequentially applies data type casting mapping across all configured anime columns."""

        for col in self.casting_map["int"]: df[col] = self._to_numeric(df[col], dtype='Int64')
        for col in self.casting_map["float"]: df[col] = self._to_numeric(df[col], dtype='Float64')
        for col in self.casting_map["string"]:  df[col] = self._to_string(df[col])
        for col in self.casting_map["boolean"]: df[col] = self._to_boolean(df[col])
        for col in self.casting_map["datetime"]: df[col] = self._to_datetime(df[col])
        for col in self.casting_map["time"] : df[col] = self._to_datetime(df[col], format='%H:%M').dt.time
        return df
    
    def _handle_incorrect_anime_data_logic(self, df) -> pd.DataFrame:
        """
        Enforces custom business rules to correct data logic contradictions coming from the API source.
        
        Protects against paradoxical timestamps, invalid scoring metrics, and irrational chronological metrics.
        """

        # Rule 1: 'Aired From' timestamp cannot happen after the 'Aired To' timestamp
        df.loc[(df["aired.from"].notna()) & (df["aired.to"].notna()) & (df["aired.from"] > df["aired.to"]),"aired.to"] = pd.NA

        # Rule 2: If a score exists but nobody evaluated the show, correct the score to zero
        df.loc[(df["score"] > 0 )& (df["scored_by"] == 0), "score"] = 0

        # Rule 3: Zero evaluators implies the score metric is non-existent 
        df.loc[df["scored_by"] == 0, "score"] = pd.NA

        # Rule 4: Episode duration must strictly be greater than zero minutes
        df.loc[df["duration_per_ep"] <= 0, "duration_per_ep"] = pd.NA

        # Rule 5: Unreleased/not yet aired anime cannot possess valid scoring properties
        df.loc[df["status"] == "not yet aired", ["scored_by","score"]] = pd.NA
        return df
    
    def _handle_missing_anime_data(self, df) -> pd.DataFrame:
        """Imputes logical baseline values for fields experiencing missing data anomalies."""

        df["popularity"] = df["popularity"].fillna(0)
        df["favorites"] = df["favorites"].fillna(0)
        df["airing"] = df["airing"].fillna(False)
        return df

    def _clean_anime_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies the comprehensive parsing, casting, and rule-checking pipeline for the main Anime entity.
        
        Args:
            df (pd.DataFrame): The raw input Silver DataFrame for Anime records.
            
        Returns:
            pd.DataFrame: A highly standardized, structurally checked, and deduplicated DataFrame.
        """
        self.logger.info("Cleaning anime table")
        df = df.copy() 
        try:

            # Parsing rating and duration
            df = self._parsing_anime(df)

            # Casting datatype
            df = self._casting_anime(df)

            # Handle incorrect data logic
            df = self._handle_incorrect_anime_data_logic(df)

            # Handle missing data
            df = self._handle_missing_anime_data(df)

            # filter final column 
            final_cols = self.casting_map["int"] + self.casting_map['float'] + self.casting_map["string"]  + self.casting_map["datetime"] + self.casting_map["time"] + self.casting_map["boolean"]
            
            return df[final_cols].drop_duplicates()
        except Exception as e:
            self.logger.error(f"**Error cleaning anime table: {e}**",exc_info=True)
            raise


    def _clean_metadata_table(self, df : pd.DataFrame) -> pd.DataFrame:
        """
        Cleans peripheral bridge/lookup tables containing ancillary entity metadata.
        
        Normalizes internal alphanumeric identifiers to explicit numbers and non-id fields to string arrays.
        
        Args:
            df (pd.DataFrame): The raw metadata DataFrame (e.g., studios, genres).
            
        Returns:
            pd.DataFrame: Deduplicated and type-standardized metadata structure.
        """
        self.logger.info("Cleaning metadata table")
        df = df.copy()
        try:
            for column in df.columns:
                if "mal_id" in column:
                    df[column] = self._to_numeric(df[column])
                else:
                    df[column] = self._to_string(df[column])

            return df.drop_duplicates()
        
        except Exception as e:
            self.logger.error(f"**Falied cleaning {df} metadata: {e}**", exc_info=True)
            raise
    

    def run_clean(self) -> dict[str, pd.DataFrame]:
        """
        Orchestrates and triggers the execution of data cleansing across the complete Silver dataset.
        
        Identifies dataset context dynamically via schema keys, applying isolated cleansing 
        tracks for either the structural Anime hub or companion categorical metadata arrays.
        
        Returns:
            dict[str, pd.DataFrame]: A mirror schema mapping tracking clean, relational DataFrames.
                                    
        """
        self.logger.info("Running clean")
        cleaned_silver_schema = {}
        try: 
            for key, df in self.silver_schema.items():
                self.logger.info(f"Cleaning {key} dataframe")
                if key == "df_anime":
                    cleaned_silver_schema[key] = self._clean_anime_table(df)
                else:
                    cleaned_silver_schema[key] = self._clean_metadata_table(df)
            self.logger.info("**Cleaning successfully**")
            return cleaned_silver_schema
        
        except Exception as e:
            self.logger.error(f"**Failed running clean {e}:**",exc_info=True)
            raise