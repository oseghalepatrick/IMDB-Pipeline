{{ config(materialized='view') }}

WITH ratings_data AS 
(
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY tconst ORDER BY numVotes DESC, averageRating DESC) AS rn
  FROM
    {{ source('staging','title_ratings_data') }}
)
SELECT
  tconst,
  cast(averageRating as numeric) as average_rating,
  cast(numVotes as integer) as number_of_votes,
  {{ get_ratings_level('averageRating') }} AS rating_level
FROM
  ratings_data
WHERE rn=1
-- order by number_of_votes DESC
-- LIMIT
--   1000