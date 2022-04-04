{{ config(materialized='view') }}
WITH title_basics as
(
  SELECT *, row_number() over(partition by tconst) as rn
  FROM
    {{ source('staging','title_basics_data') }}
)
SELECT
  tconst,
  titleType as title_type,
  primaryTitle as primary_title,
  originalTitle as original_title,
  cast(isAdult as integer) as is_adult,
  cast(startYear as integer) as start_year,
  cast(endYear as integer) as end_year,
  cast(runtimeMinutes as integer) as runtime_minutes,
  genres,
  concat("https://www.imdb.com/title/", tconst) as link
FROM
    title_basics
WHERE
  titleType IN ('movie', 'tvSeries') and rn=1
-- order by tconst
-- LIMIT 1000