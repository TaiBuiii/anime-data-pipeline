CREATE TABLE IF NOT EXISTS silver.rating(
    rating_id INT PRIMARY KEY,
    rating_code TEXT,
    rating_description TEXT
);

CREATE TABLE IF NOT EXISTS  silver.broadcast(
    broadcast_id INT PRIMARY KEY,
    broadcast_day TEXT,
    broadcast_time TIME,
    broadcast_timezone TEXT
);

CREATE TABLE IF NOT EXISTS silver.anime(
    anime_mal_id INT PRIMARY KEY,
    title TEXT NOT NULL,
    title_english TEXT,
    title_japanese TEXT,
    url TEXT,
    type TEXT,
    source TEXT,
    episodes INT,
    duration_per_ep FLOAT,
    rating_id INT,
    score FLOAT,
    scored_by INT,
    popularity INT,
    favorites INT,
    airing BOOLEAN,
    status TEXT,
    aired_from DATE, 
    aired_to DATE,
    season TEXT,
    broadcast_id INT,
    FOREIGN KEY (rating_id) REFERENCES silver.rating(rating_id),
    FOREIGN KEY(broadcast_id) REFERENCES silver.broadcast(broadcast_id)
);

CREATE TABLE IF NOT EXISTS silver.organization(
    organization_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS silver.genre(
    genre_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS silver.theme(
    theme_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS silver.demographic(
    demographic_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS silver.anime_organization(
    anime_mal_id INT NOT NULL,
    organization_mal_id INT NOT NULL,
    role TEXT, --publisher, liscensor, producer
    
    PRIMARY KEY (anime_mal_id,organization_mal_id, role),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime (anime_mal_id),
    FOREIGN KEY (organization_mal_id) REFERENCES silver.organization (organization_mal_id)
);

CREATE TABLE IF NOT EXISTS silver.anime_genre(
    anime_mal_id INT NOT NULL,
    genre_mal_id INT NOT NULL,

    PRIMARY KEY (anime_mal_id, genre_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime(anime_mal_id),
    FOREIGN KEY (genre_mal_id) REFERENCES silver.genre(genre_mal_id)
);

CREATE TABLE IF NOT EXISTS silver.anime_theme(
    anime_mal_id INT NOT NULL,
    theme_mal_id INT NOT NULL,
    PRIMARY KEY (anime_mal_id, theme_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime(anime_mal_id),
    FOREIGN KEY (theme_mal_id) REFERENCES silver.theme(theme_mal_id)
);

CREATE TABLE IF NOT EXISTS silver.anime_demographic(
    anime_mal_id INT NOT NULL,
    demographic_mal_id INT NOT NULL,
    
    PRIMARY KEY (anime_mal_id, demographic_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime(anime_mal_id),
    FOREIGN KEY (demographic_mal_id) REFERENCES silver.demographic(demographic_mal_id)
);

