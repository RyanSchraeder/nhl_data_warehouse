{{ config 
    (
        alias = 'games', 
        materialized = 'incremental', 
        incremental_strategy = 'delete+insert'
    )
}}

select *
from {{ source('raw', 'regular_season') }}
{% if is_incremental() %}
    where unique_key not in ( select unique_key from {{ this }})
{% endif %}