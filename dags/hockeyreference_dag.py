from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
from airflow.models.baseoperator import chain
from pendulum import datetime, duration, now, from_format
import os
import logging
import requests
import fireducks.pandas as pd

logger = logging.getLogger('hockey_reference_etl')
logger.setLevel(logging.DEBUG)
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout.setFormatter(formatter)
logger.addHandler(stdout)

_SNOWFLAKE_CONN_ID = "snowflake_conn"
_SNOWFLAKE_API_ID = "snowflake_api"
_SNOWFLAKE_DB = "NHL_STATS"
_SNOWFLAKE_SCHEMA = "RAW"
_SNOWFLAKE_SEASON_TABLE = "REGULAR_SEASON"
_SNOWFLAKE_TEAM_TABLE = "TEAM_STATS"
_NHL_SEASONS_URL = f"https://www.hockey-reference.com/leagues/NHL_" 
_NHL_TEAMS_URL = "https://www.hockey-reference.com/leagues/NHL_" 

def extract_data_from_nhl(**kwargs):
    season_url = f"{_NHL_SEASONS_URL}{kwargs['date']}_games.html"
    team_url = f"{_NHL_TEAMS_URL}{kwargs['date']}.html#teams"

    try:
        szn_response = requests.get(season_url)
        team_response = requests.get(team_url)
        szn_response.raise_for_status()
        team_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error fetching NHL data: {e}")

    seasons_df = pd.read_html(szn_response.text)
    teams_df = pd.read_html(team_response.text)
    output_seasons = pd.concat(seasons_df, axis=0).reset_index(drop=True)
    output_teams = pd.concat(teams_df, axis=0).reset_index(drop=True)

    try:
        output_seasons.to_csv(f'nhl_{kwargs["date"]}_output_seasons.csv', index=False)
        output_teams.to_csv(f'nhl_{kwargs["date"]}_output_teams.csv', index=False)

        print(f"File storage successful in local system: {os.listdir('.')}")

    except Exception as e:
        raise f"Issue copying files to local filesystem: {e}"
    
    return 'Successfully wrote API extracts to local files'


@dag(
    dag_display_name="Snowflake Extract & Load",
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": duration(seconds=5)},
    doc_md=__doc__,
    tags=["snowflake_etl"],
    template_searchpath=[
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/sql")
    ]  
)
def snowflake_transfer():

    extract_data = PythonOperator(
        task_id="extract_data", 
        python_callable=extract_data_from_nhl, 
        op_kwargs={
            'date': f"{now('America/Denver').year}"
        }
    )

    load_season_data_to_s3 = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename=f"nhl_{now('America/Denver').year}_output_seasons.csv",
        dest_bucket="nhl-data-raw/csv",
        dest_key=f"seasons/nhl_{now('America/Denver').year}_output_seasons.csv",
        aws_conn_id = 's3_key',
        replace=True
    )

    load_teams_data_to_s3 = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job_2",
        filename=f"nhl_{now('America/Denver').year}_output_teams.csv",
        dest_bucket='nhl-data-raw/csv',
        dest_key=f"teams/nhl_{now('America/Denver').year}_output_teams.csv",
        aws_conn_id = 's3_key',
        replace=True
    )

    set_snowflake_context = SQLExecuteQueryOperator(
        task_id="set_db",
        conn_id=_SNOWFLAKE_CONN_ID,
        database="NHL_STATS",
        sql=f"USE DATABASE {_SNOWFLAKE_DB};"
    )
    set_snowflake_schema = SQLExecuteQueryOperator(
        task_id="set_schema",
        conn_id=_SNOWFLAKE_CONN_ID,
        database="NHL_STATS",
        sql=f"USE SCHEMA {_SNOWFLAKE_SCHEMA};"
    )

    query_data = SQLExecuteQueryOperator(
        task_id="query_test",
        conn_id=_SNOWFLAKE_CONN_ID,
        database="NHL_STATS",
        sql="query_nhl_stats_season.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_SEASON_TABLE
        }
    )

    load_seasons_data = SQLExecuteQueryOperator(
        task_id="copy_into_nhl_seasons_data", 
        conn_id=_SNOWFLAKE_CONN_ID, 
        database="NHL_STATS", 
        sql="copy_into_nhl_stats_seasons.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_SEASON_TABLE,
            "source": "seasons"
        }
    )

    load_teams_data = SQLExecuteQueryOperator(
        task_id="copy_into_nhl_teams_data", 
        conn_id=_SNOWFLAKE_CONN_ID, 
        database="NHL_STATS", 
        sql="copy_into_nhl_stats_teams.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TEAM_TABLE,
            "source": "teams"
        }
    )

    # use SQLCheck operators to check the quality of your data
    # checks for null NHL team values and fails if so
    data_quality_check = SQLColumnCheckOperator(
        task_id="data_quality_check",
        conn_id=_SNOWFLAKE_CONN_ID,
        database=_SNOWFLAKE_DB,
        table=f"{_SNOWFLAKE_SCHEMA}.{_SNOWFLAKE_TEAM_TABLE}",
        column_mapping={
            "TEAM": {"null_check": {"equal_to": 0}}
        },
    )

    chain(
        extract_data,
        load_season_data_to_s3,
        load_teams_data_to_s3,
        set_snowflake_context,
        set_snowflake_schema,
        query_data,
        load_seasons_data,
        load_teams_data,
        data_quality_check
    )


snowflake_transfer()

