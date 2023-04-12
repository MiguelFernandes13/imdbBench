SELECT n.id,
    n.primary_name,
    date_part('year', NOW())::int - n.birth_year AS age,
    count(*) AS roles
FROM name n
JOIN titlePrincipals tp ON tp.name_id = n.id
JOIN titlePrincipalsCharacters tpc ON tpc.title_id = tp.title_id
    AND tpc.name_id = tp.name_id
JOIN category c ON c.id = tp.category_id
JOIN title t ON t.id = tp.title_id
LEFT JOIN titleEpisode te ON te.title_id = tp.title_id
WHERE t.start_year >= date_part('year', NOW())::int - 10
    AND c.name = 'actress'
    AND n.death_year IS NULL
    AND t.title_type IN (
        'movie', 'tvSeries', 'tvMiniSeries', 'tvMovie'
    )
    AND te.title_id IS NULL
GROUP BY n.id
ORDER BY roles DESC
LIMIT 100;
