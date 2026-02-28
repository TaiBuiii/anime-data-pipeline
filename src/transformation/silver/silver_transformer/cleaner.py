import pandas as pd
from utils.logger import get_logger
import re 

logger = get_logger(__name__)

class Cleaner:
    def __init__(self, silver_schema: dict[pd.DataFrame]):
        self.silver_schema = silver_schema
        self.logger = logger

    @staticmethod
    def _convert_duration_to_minute(series : pd.Series):
        logger.info("")
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
        try:
            rating_splits = series.str.split(" - ", n=1, expand = True)
            return rating_splits
        except Exception as e:
            logger.error(f"**Failed Spliting rating metadata: {e}**")
            raise

    @staticmethod
    def _fill_category(series: pd.Series, value="Unknown"):
        try:
            if value not in series.cat.categories:
                series = series.cat.add_categories([value])
            return series.fillna(value)
        except Exception as e:
            logger.error(f"**Failed filling category's NA value: {e}**")
            raise


    def _to_string(self, s : pd.Series): return s.astype("string").str.strip()
    def _to_category(self, s : pd.Series): return self._to_string(s).astype("category")
    def _to_numeric(self, s : pd.Series, dtype='Int64'): return pd.to_numeric(s, errors='coerce').astype(dtype)
    def _to_datetime(self, s : pd.Series, format=None): return pd.to_datetime(s, format=format, errors='coerce')


    def _clean_anime_table(self, df: pd.DataFrame):
        self.logger.info("Cleaning anime table")
        df = df.copy() 
        try:

            # 1. PARSING (Tách rating và duration trước)
            df[['rating_code','rating_description']] = self._split_rating(df["rating"])
            df.drop(columns = ['rating'], inplace = True)

            df["duration_per_ep"] = self._convert_duration_to_minute(df["duration"])

            # 2. CASTING với MAPPING 
            casting_map = {
                "int": ["anime_mal_id", "episodes", "scored_by", "popularity", "favorites"],
                "float" : ["score", "duration_per_ep"],
                "string": ["title", "title_english", "title_japanese", "url", "broadcast.timezone"],
                "category": ["broadcast.day", "type", "source", "status", "season", "rating_code", "rating_description"]
            }

            for col in casting_map["int"]: df[col] = self._to_numeric(df[col])
            for col in casting_map["float"]: df[col] = self._to_numeric(df[col], dtype='float64')
            for col in casting_map["string"]:  df[col] = self._to_string(df[col])
            for col in casting_map["category"]: df[col] = self._to_category(df[col])

            # 3. Xử lý Date/Time đặc thù
            df['aired.to'] = self._to_datetime(df['aired.to']).dt.date
            df['aired.from'] = self._to_datetime(df['aired.from']).dt.date
            df['broadcast.time'] = self._to_datetime(df['broadcast.time'], format='%H:%M').dt.time
            df['airing'] = df["airing"].astype("boolean")

            # HANDLE INCORRECT AIRING DATE
            df.loc[(df["aired.from"].notna()) & (df["aired.to"].notna()) & (df["aired.from"] > df["aired.to"]),"aired.to"] = pd.NA

            # HANDLE MISSING VALUE
            for col in casting_map["category"]: df[col] = self._fill_category(df[col])
            
            df["popularity"] = df["popularity"].fillna(0)
            df["favorites"] = df["favorites"].fillna(0)

            # 4. FILTER FINAL COLUMNS
            final_cols = casting_map["int"] + casting_map['float'] + casting_map["string"] +  casting_map["category"] + ['aired.from', 'aired.to', 'broadcast.time', 'airing']
            
            return df[final_cols]
        except Exception as e:
            self.logger.error(f"**Error cleaning anime table: {e}**")
            raise
    
    def _clean_metadata_table(self, df : pd.DataFrame):
        self.logger.info("Cleaning metadata table")
        df = df.copy()
        try:
            for column in df.columns:
                if "mal_id" in column:
                    df[column] = self._to_numeric(df[column])
                elif column == "url":
                    df[column] = self._to_string(df[column])
                else:
                    df[column] = self._to_category(df[column])
            return df
        
        except Exception as e:
            self.logger.info(f"**Falied cleaning {df} metadata: {e}**")
            raise
    

    def run_clean(self):
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