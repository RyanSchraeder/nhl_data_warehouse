{{ config 
    (
        alias = 'games'
    )
}}

select *
from {{ source('raw', 'regular_season') }}
