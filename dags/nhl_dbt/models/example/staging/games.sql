{{ config 
    (
        alias = 'games', 
        materialized = 'incremental', 
        incremental_strategy = 'delete+insert'
    )
}}

select *
from {{ source('raw', 'nhl_api_reg_schedules') }}
{% if is_incremental() %}
    where unique_key not in ( select unique_key from {{ this }})
{% endif %}