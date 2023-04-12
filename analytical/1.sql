SELECT *
FROM (
    SELECT t.id,
        left(t.primary_title, 30),
        ((start_year / 10) * 10)::int AS decade,
        avg(uh.rating) AS rating,
        rank() over (
            PARTITION by ((start_year / 10) * 10) :: int
            ORDER BY avg(uh.rating) DESC, t.id
        ) AS rank
    FROM title t
    JOIN userHistory uh ON uh.title_id = t.id
    WHERE t.title_type = 'movie'
        AND ((start_year / 10) * 10)::int >= 1980
        AND t.id IN (
            SELECT title_id
            FROM titleGenre tg
            JOIN genre g ON g.id = tg.genre_id
            WHERE g.name IN (
                'Drama'
            )
        )
        AND t.id IN (
            SELECT title_id
            FROM titleAkas
            WHERE region IN (
                'US', 'GB', 'ES', 'DE', 'FR', 'PT'
            )
        )
    GROUP BY t.id
    HAVING count(uh.rating) >= 3
    ORDER BY decade, rating DESC
) t_
WHERE rank <= 10;
