{{ config(materialized='view') }}

SELECT
  tconst,
  cast(ordering as integer) as ordering,
  nconst,
  category,
  characters
FROM
  {{ source('staging','title_principals_data') }}
WHERE
  category LIKE 'act%'
-- LIMIT
--   1000