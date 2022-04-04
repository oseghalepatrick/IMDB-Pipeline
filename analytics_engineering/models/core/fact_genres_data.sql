{{ config(materialized='table') }}

with title_basics as (
    select * 
    from {{ ref('fact_movies_data') }}
),

title_gen as (
    select 
        b.tconst,
        genre
    from {{ ref('fact_movies_data') }} as b
    join UNNEST(SPLIT(b.genres)) as genre
)
select
    title_basics.tconst,
    title_basics.genres as genres,
    title_gen.genre as genres_name,
    count(1) as numb
from title_basics
inner join title_gen
on title_basics.tconst = title_gen.tconst
group by 1, 2, 3