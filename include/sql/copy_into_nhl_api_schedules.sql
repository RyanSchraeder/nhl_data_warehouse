COPY INTO {{ params.db_name }}.{{ params.schema_name }}.{{ params.table_name }}
FROM (
    SELECT 
        metadata$start_scan_time as updated_at,
        parse_json($1) as raw_json, 
        md5(metadata$filename || raw_json) as unique_key,
        metadata$filename as source_file,
        raw_json['games'] as games, 
        raw_json['league'] as league, 
        raw_json['season'] as season
    FROM @{{ params.db_name }}.{{ params.schema_name }}.nhl_raw_data/json/{{ params.season_type }}
)
FILE_FORMAT = {{ params.db_name }}.{{ params.schema_name }}.json
PATTERN = '.*json.*'
;