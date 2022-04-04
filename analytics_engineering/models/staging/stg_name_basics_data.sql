{{ config(materialized='view') }}

WITH name_basics AS
(
  SELECT *,
    row_number() over(partition by nconst order by nconst) as rn
  FROM
    {{ source('staging','name_basics_data') }}
  WHERE
  primaryProfession LIKE '%act%'
)
SELECT
  nconst,
  primaryName AS act_name,
  CAST(birthYear AS integer) AS birth_year,
  CAST(deathYear AS integer) AS death_year,
  primaryProfession AS primary_profession,
  knownForTitles AS known_for_titles --EXCEPT(profession)
FROM
  name_basics AS star_names
-- JOIN
--   UNNEST(SPLIT(star_names.primaryProfession)) AS profession
WHERE
  rn=1
ORDER BY
  nconst
-- LIMIT
--   100000