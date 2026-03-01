import pandas as pd
from utils.logger import get_logger
import re 

logger = get_logger(__name__)

class Cleaner:
    def __init__(self, silver_schema: dict[pd.DataFrame]):
        self.silver_schema = silver_schema
        self.logger = logger
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
        logger.info("Converting duration to minute")
        try:
            pattern = r'(?:(\d+)\s*hr)?\s*(?:(\d+)\s*min)?\s*(?:(\d+)\s*sec)?'
            extracted = series.str.extract(pattern)

            hours = pd.to_numeric(extracted[0], errors="coerce").fillna(0)
            minutes = pd.to_numeric(extracted[1], errors="coerce").fillna(0)
            seconds = pd.to_numeric(extracted[2], errors="coerce").fillna(0)

            return hours*60 + minutes + seconds/60
        
        except Exception as e:
            logger.error(f"**Faile converting duration to minute: {e}**")
            raise

    @staticmethod
    def _split_rating(series : pd.Series) -> pd.DataFrame:
        logger.info("Spliting rating")
        try:
            rating_splits = series.str.split(" - ", n=1, expand = True)
            return rating_splits
        except Exception as e:
            logger.error(f"**Failed Spliting rating metadata: {e}**")
            raise

    @staticmethod       
    def _to_string(s : pd.Series) -> pd.Series: 
        return s.astype("string").str.strip().str.lower()
    
    @staticmethod
    def _to_numeric(s : pd.Series, dtype='Int64') -> pd.Series: 
        return s.astype(dtype)

    @staticmethod
    def _to_boolean(s: pd.Series) -> pd.Series:
        return pd.to_numeric(s.replace({'true': "1", 'false': "0"}).fillna("0")).astype("boolean")
        
    @staticmethod
    def _to_datetime(s : pd.Series, format=None) -> pd.Series: 
        return pd.to_datetime(s, format=format, errors='coerce')

    def parsing_anime(self, df) -> pd.DataFrame:
        # Split rating into rating code and rating description
        df[['rating_code','rating_description']] = self._split_rating(df["rating"])
        df.drop(columns = ['rating'], inplace = True)

        # Convert duration to minutes
        df["duration_per_ep"] = self._convert_duration_to_minute(df["duration"])
        df.drop(columns = ["duration"], inplace = True)

        return df

    def casting_anime(self, df) -> pd.DataFrame:
        for col in self.casting_map["int"]: df[col] = self._to_numeric(df[col], dtype='Int64')
        for col in self.casting_map["float"]: df[col] = self._to_numeric(df[col], dtype='Float64')
        for col in self.casting_map["string"]:  df[col] = self._to_string(df[col])
        for col in self.casting_map["boolean"]: df[col] = self._to_boolean(df[col])
        for col in self.casting_map["datetime"]: df[col] = self._to_datetime(df[col])
        for col in self.casting_map["time"] : df[col] = self._to_datetime(df[col], format='%H:%M').dt.time
        return df
    
    def handle_incorrect_anime_data_logic(self, df) -> pd.DataFrame:
        df.loc[(df["aired.from"].notna()) & (df["aired.to"].notna()) & (df["aired.from"] > df["aired.to"]),"aired.to"] = pd.NA
        df.loc[(df["score"] > 0 )& (df["scored_by"] == 0), "score"] = 0
        df.loc[df["scored_by"] == 0, "score"] = pd.NA
        df.loc[df["duration_per_ep"] <= 0, "duration_per_ep"] = pd.NA
        df.loc[df["status"] == "not yet aired", ["scored_by","score"]] = pd.NA
        return df
    
    def handle_missing_anime_data(self, df) -> pd.DataFrame:
        df["popularity"] = df["popularity"].fillna(0)
        df["favorites"] = df["favorites"].fillna(0)
        df["airing"] = df["airing"].fillna(False)
        return df

    def _clean_anime_table(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Cleaning anime table")
        df = df.copy() 
        try:

            # Parsing rating and duration
            df = self.parsing_anime(df)

            # Casting datatype
            df = self.casting_anime(df)

            # Handle incorrect data logic
            df = self.handle_incorrect_anime_data_logic(df)

            # Handle missing data
            df = self.handle_missing_anime_data(df)

            # 4. FILTER FINAL COLUMNS
            final_cols = self.casting_map["int"] + self.casting_map['float'] + self.casting_map["string"]  + self.casting_map["datetime"] + self.casting_map["time"] + self.casting_map["boolean"]
            
            return df[final_cols].drop_duplicates()
        except Exception as e:
            self.logger.error(f"**Error cleaning anime table: {e}**")
            raise


    def _clean_metadata_table(self, df : pd.DataFrame) -> pd.DataFrame:
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
            self.logger.info(f"**Falied cleaning {df} metadata: {e}**")
            raise
    

    def run_clean(self) -> dict[str, pd.DataFrame]:
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
            self.logger.error(f"**Failed running clean {e}:**")
            raise