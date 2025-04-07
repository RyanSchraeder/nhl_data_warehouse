from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
from airflow.models.baseoperator import chain
from airflow.hooks.base import BaseHook
from pendulum import datetime, duration, now
import os
import requests
import duckdb
import fireducks.pandas as pd
import logging
import json
from pprint import pprint

logger = logging.getLogger('nhl_api_etl')
logger.setLevel(logging.DEBUG)
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout.setFormatter(formatter)
logger.addHandler(stdout)


# JSON Schema & Data Model
'''TODO: 
    - Seasons X
    - Teams 
    - Schedule X
    - Player Profile
    - Team Profile
    - Game Analytics
'''
# Seasons: Gets all seasons and ids by league 
# |_______________ PK: SEASON_ID, LEAGUE_ID
# |_______________ No request requirement
# Teams:  provides a complete list of active teams in the NHL API database.
# |_______________ PK: TEAM_ID, FK: LEAGUE_ID
# |_______________ No request requirement
# Schedule: provides date, time, location, and other event details for every match-up taking place in the full league season. Can be called for regular or postseason.
# |_______________ PK: GAME_ID, FK: LEAGUE_ID, TEAM_ID, SEASON_ID
# |_______________ Requires season_year, season_type for API request
# Player Profile: provides player biographical information, draft information, and seasonal statistics.
# |_______________ PK: PLAYER_ID, FK: LEAGUE_ID, TEAM_ID, SEASON_ID
# |_______________ Requires player_id for API request
# Team Profile: NHL Team Profile provides top-level team information and a full roster of active players.
# |_______________ PK: TEAM_ID, FK: LEAGUE_ID, PLAYER_ID, TEAM_ID, SEASON_ID
# |_______________ Requires team_id for API request
# Game Analytics: NHL Game Analytics provides detailed team and player analytics for a given game, including corsi, fenwick, on ice shots, and shots by type.
# |_______________ PK: GAME_ID, FK: LEAGUE_ID, PLAYER_ID, TEAM_ID, SEASON_ID
# |_______________ Requires game_id for API request

_SNOWFLAKE_CONN_ID = "snowflake_conn"
_SNOWFLAKE_API_ID = "snowflake_api"
_SNOWFLAKE_DB = "NHL_STATS"
_SNOWFLAKE_SCHEMA = "RAW"
_SNOWFLAKE_SEASON_TABLE = "NHL_API_REG_SCHEDULES"
_SNOWFLAKE_PLAYOFFS_TABLE = "NHL_API_PLAYOFF_SCHEDULES"
_SNOWFLAKE_TEAMS_TABLE = "NHL_API_TEAMS"
_NHL_API_CONN = BaseHook.get_connection('nhl_api_key')
_NHL_API_KEY = _NHL_API_CONN.password
_NHL_SEASON_SCHEDULE_URL = "https://api.sportradar.com/nhl/trial/v7/en/games"
_NHL_SEASONS_URL = "https://api.sportradar.com/nhl/trial/v7/en/league/seasons.json"
_NHL_TEAMS_URL = "https://api.sportradar.com/nhl/trial/v7/en/league/teams.json"
_PROCESS_DATE = now('America/Denver').to_date_string()


def extract_from_api(url, date, season_type, endpoint):

    url = f"{url}?api_key={_NHL_API_KEY}"
    filename = f'nhl_api_extract_{endpoint}_{date}'
    headers = {"accept": "application/json"}

    try:
        if endpoint == "schedule":
            url = f"{_NHL_SEASON_SCHEDULE_URL}/{date}/{season_type}/schedule.json?api_key={_NHL_API_KEY}"
            logger.info(f'URL Built: {url}')
            filename = f'nhl_api_{season_type}_extract_{endpoint}_{_PROCESS_DATE}'
            logger.info(f'Filepath: {filename}')
        
        if endpoint == 'seasons':
            url = f"{_NHL_SEASONS_URL}?api_key={_NHL_API_KEY}"
            filename = f'nhl_api_extract_seasons_{date}'
            logger.info(f'Filepath: {filename}')
        
        response = requests.get(url, headers=headers)
        content = response.json()

        if endpoint == "schedule" and 'games' not in content.keys():
            return logger.info(f"No data found. No data saved to file")
        else:
            with open(f'{filename}.json', 'w', encoding='utf-8') as file:
                json.dump(content, file, indent=4)

        return logger.info("Successful retrieval of data.")
       
    except Exception as e:
        raise logger.error(f"Issue occurred extracting from the NHL API: {e}")

@dag(
    dag_display_name="NHL API Extract & Load",
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": duration(seconds=5)},
    doc_md=__doc__,
    tags=["nhl_api_etl"],
    template_searchpath=[
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/sql")
    ]
)


def task_run():

    seasons_history = PythonOperator(
        task_id="seasons_extract",
        python_callable=extract_from_api,
        op_kwargs={
            'url': _NHL_SEASONS_URL,
            'date': now('America/Denver').year - 1,
            'endpoint': 'seasons',
            'season_type': ''
        }
    )

    team_heirarchy = PythonOperator(
        task_id="teams_extract",
        python_callable=extract_from_api,
        op_kwargs={
            'url': _NHL_TEAMS_URL,
            'date': now('America/Denver').year - 1,
            'endpoint': 'teams',
            'season_type': ''
        }
    )

    regular_season_games = PythonOperator(
        task_id="reg_season_extract",
        python_callable=extract_from_api,
        op_kwargs={
            'url': _NHL_SEASON_SCHEDULE_URL,
            'date': now('America/Denver').year - 1,
            'season_type': 'REG',
            'endpoint': 'schedule'
        }
    )

    post_season_games = PythonOperator(
        task_id="pst_season_extract",
        python_callable=extract_from_api,
        op_kwargs={
            'url': _NHL_SEASON_SCHEDULE_URL,
            'date': now('America/Denver').year - 1,
            'season_type': 'PST',
            'endpoint': 'schedule'
        }
    )

    check_for_reg_file = FileSensor(
        task_id='check_for_reg_file',
        filepath=f'nhl_api_REG_extract_schedule_{_PROCESS_DATE}.json',
        poke_interval=10,  # Seconds between checks
        timeout=10,  # Maximum waiting time in seconds
        retries=1
    )

    check_for_pst_file = FileSensor(
        task_id='check_for_pst_file',
        filepath=f'nhl_api_PST_extract_schedule_{_PROCESS_DATE}.json',
        poke_interval=10,  # Seconds between checks
        timeout=10,  # Maximum waiting time in seconds
        retries=1
    )
    
    load_seasons_json_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_seasons_json_to_s3",
        filename=f'nhl_api_extract_seasons_{now('America/Denver').year - 1}.json',
        dest_bucket="nhl-data-raw",
        dest_key=f"json/seasons/nhl_api_extract_seasons_{now('America/Denver').year - 1}.json",
        aws_conn_id = 's3_key',
        replace=True
    )

    load_teams_json_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_teams_json_to_s3",
        filename=f'nhl_api_extract_teams_{now('America/Denver').year - 1}.json',
        dest_bucket="nhl-data-raw",
        dest_key=f"json/teams/nhl_api_extract_teams_{now('America/Denver').year - 1}.json",
        aws_conn_id = 's3_key',
        replace=True
    )

    load_reg_season_json_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_reg_season_schedule_json_to_s3",
        filename=f'nhl_api_REG_extract_schedule_{_PROCESS_DATE}.json',
        dest_bucket="nhl-data-raw",
        dest_key=f"json/regular_season/nhl_api_REG_extract_schedule_{_PROCESS_DATE}.json",
        aws_conn_id = 's3_key',
        replace=True
    )

    load_playoff_season_json_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_playoff_season_schedule_json_to_s3",
        filename=f'nhl_api_PST_extract_schedule_{_PROCESS_DATE}.json',
        dest_bucket="nhl-data-raw",
        dest_key=f"json/post_season/nhl_api_PST_extract_schedule_{_PROCESS_DATE}.json",
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
    
    load_seasons_data = SQLExecuteQueryOperator(
        task_id="copy_into_nhl_api_seasons", 
        conn_id=_SNOWFLAKE_CONN_ID, 
        database="NHL_STATS", 
        sql="copy_into_nhl_api_seasons.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA
        }
    )

    load_reg_season_data = SQLExecuteQueryOperator(
        task_id="copy_into_nhl_api_regular_season_schedules", 
        conn_id=_SNOWFLAKE_CONN_ID, 
        database="NHL_STATS", 
        sql="copy_into_nhl_api_schedules.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_SEASON_TABLE,
            "season_type": "regular_season"
        }
    )

    load_pst_season_data = SQLExecuteQueryOperator(
        task_id="copy_into_nhl_api_playoff_season_schedules", 
        conn_id=_SNOWFLAKE_CONN_ID, 
        database="NHL_STATS", 
        sql="copy_into_nhl_api_schedules.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_PLAYOFFS_TABLE,
            "season_type": "post_season"
        }
    )

    load_teams_data = SQLExecuteQueryOperator(
        task_id="copy_into_nhl_api_teams", 
        conn_id=_SNOWFLAKE_CONN_ID, 
        database="NHL_STATS", 
        sql="copy_into_nhl_api_teams.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TEAMS_TABLE
        }
    )

    chain(
        seasons_history,
        team_heirarchy,
        regular_season_games,
        post_season_games,
        load_seasons_json_to_s3,
        load_teams_json_to_s3,
        regular_season_games >> check_for_reg_file >> load_reg_season_json_to_s3,
        post_season_games >> check_for_pst_file >> load_playoff_season_json_to_s3,
        set_snowflake_context,
        set_snowflake_schema,
        load_seasons_data,
        load_reg_season_data,
        load_pst_season_data,
        load_teams_data
    )

task_run()