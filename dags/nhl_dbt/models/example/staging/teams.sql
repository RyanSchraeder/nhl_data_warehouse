{{ config 
    (
        alias = 'teams'
    )
}}

select 
    unique_key, 
    updated_at,
    source_file,
    league['alias']::varchar as league_name,
    league['id']::varchar as league_id,
    f.value['alias']::varchar as team_state_code,
    f.value['id']::varchar as team_id,
    f.value['market']::varchar || ' ' || f.value['name']::varchar team_name
from {{ source('raw', 'nhl_api_teams') }},
lateral flatten (input=>teams) f
