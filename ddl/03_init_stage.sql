/*
This scripts creates stage.anime_raw and stage.anime_pagination_log
, storing raw data from jikan API
*/

CREATE TABLE stage.anime_raw(
    mal_id INT,
    page INT,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE TABLE stage.anime_pagination_log(
    page INT,
    last_visible_page INT,
    has_next_page BOOLEAN,
    items_count INT,
    items_total INT,
    items_per_page INT,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);