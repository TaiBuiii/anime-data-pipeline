CREATE OR REPLACE VIEW gold.genre_trend AS 

WITH genre_yearly AS (
  SELECT 
      g.name AS genre, 
      EXTRACT(YEAR FROM a.aired_from) AS release_year,
      COUNT(a.anime_mal_id) AS total_anime_genre,
      ROUND(AVG(a.score)::NUMERIC,2) AS avg_score,
      SUM(a.favorites) AS total_favorites
  FROM silver.anime a
  INNER JOIN silver.anime_genre ag
      ON a.anime_mal_id = ag.anime_mal_id
  INNER JOIN silver.genre g
      ON ag.genre_mal_id = g.genre_mal_id
  WHERE a.aired_from IS NOT NULL AND a.score IS NOT NULL
  GROUP BY g.name, EXTRACT(YEAR FROM a.aired_from)
)
SELECT 
    genre,
    release_year,
    total_anime_genre,
    avg_score,
    total_favorites,
    ROUND(total_anime_genre*100 / SUM(total_anime_genre) OVER(PARTITION BY release_year),2) AS market_share_percent
FROM genre_yearly gy
WHERE release_year >= 2000
ORDER BY release_year DESC, genre ASC;