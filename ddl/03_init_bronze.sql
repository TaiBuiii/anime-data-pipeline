/*
This scripts creates bronze.anime_raw and bronze.anime_pagination_log
, storing raw data from jikan API
*/

CREATE TABLE bronze.anime_raw(
    mal_id INT PRIMARY KEY,
    page INT,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
    PRIMARY KEY (mal_id, page)
);

CREATE TABLE bronze.anime_pagination_log(
    page INT PRIMARY KEY,
    last_visible_page INT,
    has_next_page BOOLEAN,
    items_count INT,
    items_total INT,
    items_per_page INT,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);