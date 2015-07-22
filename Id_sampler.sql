/* Sample Users by ID
   User_id is expected to be a roughly continue sequence
   This method will return less samples due to collision*/ 

-- get the sample size (10%)
SELECT trunc(COUNT(*)*0.1)::integer AS ct
FROM users_table;
 
-- sampling
WITH params AS
(
  SELECT COUNT(*) AS ct,
         MIN(id) AS min_id,
         MAX(id) AS max_id,
         MAX(id) -MIN(id) AS id_span
  FROM users_table
),
series AS
(
  SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS n
  FROM users_table,
       params p LIMIT 4099553
)
SELECT DISTINCT id as user_id
FROM (SELECT p.min_id + trunc(random ()*p.id_span)::INTEGER AS id
      FROM params p,
           series s
      GROUP BY 1) r
  JOIN users_table USING (id)
LIMIT 1000;