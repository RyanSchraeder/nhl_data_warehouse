COPY INTO {{ params.db_name }}.{{ params.schema_name }}.{{ params.table_name }}
FROM (
    SELECT 
        METADATA$START_SCAN_TIME AS UPDATED_AT,
        METADATA$FILENAME AS SOURCE_FILE,
        MD5(METADATA$FILENAME || $1) as UNIQUE_KEY,
        trim(replace($1::varchar, '"', ''))::date as GAME_DATE,
        trim(replace($2::varchar, '"', ''))::time as GAME_TIME,
        trim(replace($3::varchar, '"', ''))::varchar as VISITOR,
        trim(replace($4::varchar, '"', ''))::number AS VISITOR_GOALS,
        trim(replace($5::varchar, '"', ''))::varchar AS HOME,
        trim(replace($6::varchar, '"', ''))::number AS HOME_GOALS,
        trim(replace($8::varchar, '"', ''))::number AS GUESTS_IN_ATTENDANCE,
        trim(replace($9::varchar, '"', ''))::time AS LENGTH_OF_GAME
    FROM @{{ params.db_name }}.{{ params.schema_name }}.nhl_raw_data/csv/{{ params.source }}/
)
FILE_FORMAT = {{ params.db_name }}.{{ params.schema_name }}.csv
PATTERN = '.*csv.*'
;