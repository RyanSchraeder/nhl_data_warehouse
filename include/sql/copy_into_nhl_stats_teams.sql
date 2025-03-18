COPY INTO {{ params.db_name }}.{{ params.schema_name }}.{{ params.table_name }}
FROM (
    SELECT 
        $1 AS TEAM,
        $2 AS GP, 
        $3 AS OVERALL_WINS,
        $4 AS OVERALL_LOSSES,
        $5 AS OVERTIME_LOSSES,
        $6 AS TOTAL_POINTS, 
        $7 AS POINTS_PERCENTAGE,
        $8 AS GOALS_FOR,
        $9 AS GOALS_AGAINST,
        $10 AS HOCKEY_REFERENCE_SRS,
        $11 AS STRENGTH_OF_SCHEDULE,
        $12 AS POINTS_PERCENTAGE_IN_REGULATION,
        $13 AS WINS_IN_REGULATION,
        $14 AS REGULATION_RECORD,
        METADATA$START_SCAN_TIME AS UPDATED_AT, 
        MD5(METADATA$FILENAME || TEAM) AS UNIQUE_TEAM_KEY
    FROM @{{ params.db_name }}.{{ params.schema_name }}.nhl_raw_data_csv/{{ params.source }}/
)
FILE_FORMAT = {{ params.db_name }}.{{ params.schema_name }}.csv
PATTERN = '.*csv.*'
;