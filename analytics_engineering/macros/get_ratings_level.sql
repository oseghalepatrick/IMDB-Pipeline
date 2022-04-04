{#
    This macro returns the description of the ratings_level by classifying it
#}

{% macro get_ratings_level(averageRating) -%}

    CASE 
        WHEN averageRating >= 8 THEN 5
        WHEN averageRating >= 6 THEN 4
        WHEN averageRating >= 4 THEN 3
        WHEN averageRating >= 2 THEN 2
    ELSE
    1
    END

{%- endmacro %}