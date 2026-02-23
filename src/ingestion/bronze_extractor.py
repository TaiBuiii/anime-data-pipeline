
def extract_pagination(data : dict) -> list:
    """
    This function is called by run_ingestion() to extract the metadata in each page,
    as a list of tuple. This returned datatype facilitates the loading process

    It returns data for bronze.anime_pagination_log
    """
    return [(
        data["current_page"],
        data["last_visible_page"],
        data["has_next_page"],
        data["items"]["count"],
        data["items"]["total"],
        data["items"]["per_page"],
    )]

def extract_anime_raw(data : list, page : int) -> list:
    """
    This function is called by run_ingestion() to extract all the records,
    contained in the page, as a list of tuple. This returned datatype 
    facilitates the loading process

    It returns data for bronze.anime_raw
    """
    return [
        (record["mal_id"], page, record) for record in data
    ]