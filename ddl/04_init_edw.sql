CREATE TABLE IF NOT EXISTS edw.rating(
    rating_id INT PRIMARY KEY,
    rating_code TEXT,
    rating_description TEXT
);

CREATE TABLE IF NOT EXISTS  edw.broadcast(
    broadcast_id INT PRIMARY KEY,
    broadcast_day TEXT,
    broadcast_time TIME,
    broadcast_timezone TEXT
);

CREATE TABLE IF NOT EXISTS edw.anime(
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
    FOREIGN KEY (rating_id) REFERENCES edw.rating(rating_id),
    FOREIGN KEY(broadcast_id) REFERENCES edw.broadcast(broadcast_id)
);

CREATE TABLE IF NOT EXISTS edw.organization(
    organization_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS edw.genre(
    genre_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS edw.theme(
    theme_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS edw.demographic(
    demographic_mal_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT
);

CREATE TABLE IF NOT EXISTS edw.anime_organization(
    anime_mal_id INT NOT NULL,
    organization_mal_id INT NOT NULL,
    role TEXT, --publisher, liscensor, producer
    
    PRIMARY KEY (anime_mal_id,organization_mal_id, role),
    FOREIGN KEY (anime_mal_id) REFERENCES edw.anime (anime_mal_id),
    FOREIGN KEY (organization_mal_id) REFERENCES edw.organization (organization_mal_id)
);

CREATE TABLE IF NOT EXISTS edw.anime_genre(
    anime_mal_id INT NOT NULL,
    genre_mal_id INT NOT NULL,

    PRIMARY KEY (anime_mal_id, genre_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES edw.anime(anime_mal_id),
    FOREIGN KEY (genre_mal_id) REFERENCES edw.genre(genre_mal_id)
);

CREATE TABLE IF NOT EXISTS edw.anime_theme(
    anime_mal_id INT NOT NULL,
    theme_mal_id INT NOT NULL,
    PRIMARY KEY (anime_mal_id, theme_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES edw.anime(anime_mal_id),
    FOREIGN KEY (theme_mal_id) REFERENCES edw.theme(theme_mal_id)
);

CREATE TABLE IF NOT EXISTS edw.anime_demographic(
    anime_mal_id INT NOT NULL,
    demographic_mal_id INT NOT NULL,
    
    PRIMARY KEY (anime_mal_id, demographic_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES edw.anime(anime_mal_id),
    FOREIGN KEY (demographic_mal_id) REFERENCES edw.demographic(demographic_mal_id)
);

