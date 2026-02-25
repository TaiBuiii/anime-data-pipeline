CREATE TABLE IF NOT EXISTS silver.anime(
    mal_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    title_english VARCHAR(255),
    title_japanese VARCHAR(255),
    url VARCHAR(255),
    type VARCHAR(50),
    source VARCHAR(50),
    episodes INT,
    duration INT,
    rating VARCHAR(50),
    score FLOAT,
    scored_by INT,
    popularity INT,
    favorites INT,
    airing BOOLEAN,
    status VARCHAR(50),
    aired_from TIMESTAMP, 
    aired_to TIMESTAMP,
    season VARCHAR(20),
    broadcast_day VARCHAR(20),
    broadcast_time TIME,
    broadcast_timezone VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS silver.organization(
    mal_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS silver.genre(
    mal_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS silver.theme(
    mal_id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    url VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS silver.demographic(
    mal_id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    url VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS silver.anime_organization(
    anime_mal_id INT NOT NULL,
    organization_mal_id INT NOT NULL,
    role VARCHAR(50), --publisher, liscensor, producer
    
    PRIMARY KEY (anime_mal_id,organization_mal_id, role),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime (mal_id),
    FOREIGN KEY (organization_mal_id) REFERENCES silver.organization (mal_id)
);

CREATE TABLE IF NOT EXISTS silver.anime_genre(
    anime_mal_id INT NOT NULL,
    genre_mal_id INT NOT NULL,

    PRIMARY KEY (anime_mal_id, genre_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime(mal_id),
    FOREIGN KEY (genre_mal_id) REFERENCES silver.genre(mal_id)
);

CREATE TABLE IF NOT EXISTS silver.anime_theme(
    anime_mal_id INT NOT NULL,
    theme_mal_id INT NOT NULL,
    PRIMARY KEY (anime_mal_id, theme_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime(mal_id),
    FOREIGN KEY (theme_mal_id) REFERENCES silver.theme(mal_id)
);

CREATE TABLE IF NOT EXISTS silver.anime_demographic(
    anime_mal_id INT NOT NULL,
    demographic_mal_id INT NOT NULL,
    
    PRIMARY KEY (anime_mal_id, demographic_mal_id),
    FOREIGN KEY (anime_mal_id) REFERENCES silver.anime(mal_id),
    FOREIGN KEY (demographic_mal_id) REFERENCES silver.demographic(mal_id)
);

