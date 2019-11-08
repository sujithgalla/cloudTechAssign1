--task 1
SELECT id,score FROM stackoverflow_posts ORDER BY score DESC LIMIT 10;

--task 2
SELECT owneruserid, SUM(score) AS total_score  FROM stackoverflow_posts
WHERE owneruserid IS NOT NULL GROUP BY owneruserid
ORDER BY total_score DESC LIMIT 10;

--task 3
SELECT COUNT(DISTINCT owneruserid) FROM stackoverflow_posts

WHERE body LIKE '%Hadoop%'
;
