SELECT t.id, t.primary_title, tg.genres, te.season_number, count(*) AS views
FROM title t
JOIN titleEpisode te ON te.parent_title_id = t.id
JOIN title t2 ON t2.id = te.title_id
JOIN userHistory uh ON uh.title_id = t2.id
JOIN users u ON u.id = uh.user_id
JOIN (
    SELECT tg.title_id, array_agg(g.name) AS genres
    FROM titleGenre tg
    JOIN genre g ON g.id = tg.genre_id
    GROUP BY tg.title_id
) tg ON tg.title_id = t.id
WHERE t.title_type = 'tvSeries'
    AND uh.last_seen BETWEEN NOW() - INTERVAL '30 days' AND NOW()
    AND te.season_number IS NOT NULL
    AND u.country_code NOT IN ('US', 'GB')
GROUP BY t.id, t.primary_title, tg.genres, te.season_number
ORDER BY count(*) DESC, t.id
LIMIT 100;
