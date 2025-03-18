COPY INTO {{ params.db_name }}.{{ params.schema_name }}.{{ params.table_name }}
FROM (
    SELECT 
        METADATA$START_SCAN_TIME AS UPDATED_AT,
        MD5(METADATA$FILENAME || $1::date) as UNIQUE_GAME_KEY,
        $1::date as GAME_DATE,
        $2 as GAME_TIME,
        $3 as VISITOR,
        $4 AS VISITOR_GOALS,
        $5 AS HOME,
        $6 AS HOME_GOALS,
        $8 AS GUESTS_IN_ATTENDANCE,
        $9 AS LENGTH_OF_GAME
    FROM @{{ params.db_name }}.{{ params.schema_name }}.nhl_raw_data_csv/{{ params.source }}/
)
FILE_FORMAT = {{ params.db_name }}.{{ params.schema_name }}.csv
PATTERN = '.*csv.*'
;