import requests 
import pandas as pd
import time
from utils.logger import get_logger


logger = get_logger(__name__)
BASE_URL = "https://api.jikan.moe/v4"

def fetch_page_data(page : int = 1) -> dict:
    """
    This function is called by run_ingestion() to get raw json file from the specified page

    Args:
    page: page number that being fetched
    
    Return:
    json file as python dictionary
    """
    url = f"{BASE_URL}/anime"
    params = {"page" : page}

    # try 4 times if any error occurs
    for attempt in range(4):
        try:

            # Call API
            response = requests.get(url, params= params, timeout = 10)

            # return the json file if status code is 200 OK
            if response.status_code == 200:
                return response.json()
            
            # idle for 2 second if rate limited
            elif response.status_code == 429:
                logger.warning("Rate limited. Sleeping 2s...")
                time.sleep(2)

            # else return specific status code
            else:
                raise Exception(f"HTTP {response.status_code}")
        
        # error occur
        except Exception as e:
            logger.error(f"Attempted {attempt + 1} failed: {e}")
            time.sleep(1)
        
    raise Exception("Max retries exceeded")
        


if __name__ == "__main__":
    fetch_page_data(1)