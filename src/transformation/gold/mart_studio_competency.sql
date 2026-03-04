CREATE OR REPLACE VIEW gold.mart_studio_competency AS

SELECT 
    g.name AS genre_name,
    o.name AS studio_name,
    r.rating_code AS rating_label,
    COUNT(DISTINCT a.anime_mal_id) AS total_anime,
    ROUND(AVG(a.score)::numeric,2) AS avg_score,
    ROUND(COUNT(CASE WHEN a.score > 7.5 THEN 1 END) * 100.0 / COUNT(a.anime_mal_id), 2) AS high_tier_rate,
    ROUND(COUNT(CASE WHEN a.score > 6.5 AND a.score <= 7.5  THEN 1 END) * 100.0 / COUNT(a.anime_mal_id), 2) AS mid_tier_rate,
    SUM(a.favorites) AS total_member,
    ROUND(AVG(a.favorites), 0) AS avg_favorites_per_anime
FROM silver.anime a
INNER JOIN silver.anime_genre ag
    ON ag.anime_mal_id = a.anime_mal_id
INNER JOIN silver.genre g
    ON ag.genre_mal_id = g.genre_mal_id
INNER JOIN silver.rating r
    ON r.rating_id = a.rating_id
INNER JOIN silver.anime_organization ao  
    ON ao.anime_mal_id = a.anime_mal_id
INNER JOIN silver.organization o 
    ON o.organization_mal_id = ao.organization_mal_id
WHERE ao."role" = 'studio'
GROUP BY g.name, o.name, r.rating_code
HAVING COUNT(a.anime_mal_id) >= 2
ORDER BY avg_score DESC NULLS LAST;
