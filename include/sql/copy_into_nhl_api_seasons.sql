COPY INTO {{ params.db_name }}.{{ params.schema_name }}.NHL_API_SEASONS
FROM (
    SELECT 
        metadata$start_scan_time as updated_at,
        parse_json($1) as raw_json, 
        md5(metadata$filename || raw_json) as unique_key, 
        raw_json['league'] as league, 
        raw_json['seasons'] as seasons
    FROM @{{ params.db_name }}.{{ params.schema_name }}.nhl_raw_data/json/seasons
)
FILE_FORMAT = {{ params.db_name }}.{{ params.schema_name }}.json
PATTERN = '.*json.*'
;
