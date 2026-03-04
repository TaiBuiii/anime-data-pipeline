CREATE OR REPLACE VIEW gold.release_timing AS

SELECT 
    b.broadcast_day,
    TO_CHAR(b.broadcast_time, 'HH24:MI') AS broadcast_time,
    ROUND(AVG(a.score)::numeric,2) AS avg_score,
    SUM(a.favorites) AS total_favorites,
    ROUND(AVG(a.favorites), 0) AS avg_favorites,
    ROUND(
        COUNT(CASE WHEN a.favorites > 7000 THEN 1 END) * 100.0 / NULLIF(COUNT(a.anime_mal_id), 0), 
        2
    ) AS high_engagement_rate
    
FROM silver.anime a
INNER JOIN silver.broadcast b 
    ON a.broadcast_id = b.broadcast_id
WHERE b.broadcast_day IS NOT NULL 
    AND b.broadcast_time IS NOT NULL 
    AND a.score IS NOT NULL 
    AND a.favorites > 100
    
GROUP BY b.broadcast_day, TO_CHAR(b.broadcast_time, 'HH24:MI')
HAVING COUNT(DISTINCT a.anime_mal_id) >= 2
ORDER BY total_favorites DESC NULLS LAST;



