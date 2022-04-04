{{ config(materialized='table') }}

with title_basics as (
    select * 
    from {{ ref('stg_title_basics_data') }}
),

title_ratings as (
    select *
    from {{ ref('stg_title_ratings_data')}}
)

select
    title_basics.tconst,
    title_type,
    primary_title,
    original_title,
    is_adult,
    start_year,
    end_year,
    runtime_minutes,
    genres,
    average_rating,
    number_of_votes,
    rating_level,
    link
from title_basics
inner join title_ratings
on title_basics.tconst = title_ratings.tconst
-- order by rating_level desc, number_of_votes desc