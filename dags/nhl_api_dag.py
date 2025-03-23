from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
from airflow.models.baseoperator import chain
from airflow.hooks.base import BaseHook
from pendulum import datetime, duration, now, from_format
import os
import requests
import duckdb
import pandas as pd
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


_SNOWFLAKE_CONN_ID = "snowflake_conn"
_SNOWFLAKE_API_ID = "snowflake_api"
_SNOWFLAKE_DB = "NHL_STATS"
_SNOWFLAKE_SCHEMA = "RAW"
_SNOWFLAKE_SEASON_TABLE = "REGULAR_SEASON"
_SNOWFLAKE_TEAM_TABLE = "TEAM_STATS"
_NHL_API_CONN = BaseHook.get_connection('nhl_api_key')
_NHL_API_KEY = _NHL_API_CONN.password
_NHL_SEASONS_URL = "https://api.sportradar.com/nhl/trial/v7/en/games"
_NHL_TEAMS_URL = ""



def extract_from_api(url, date, season_type, endpoint):

    try:
        if endpoint == "schedule":
            url = f"{_NHL_SEASONS_URL}/{date}/{season_type}/schedule.json?api_key={_NHL_API_KEY}"
            logger.info(f'URL Built: {url}')
            filename = f'nhl_api_{date}_extract_{season_type}_{endpoint}'
            logger.info(f'Filepath: {filename}')

        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        content = response.json()

        logger.info(pprint(content))

        with open(f'{filename}.json', 'w', encoding='utf-8') as file:
            json.dump(content, file, indent=4)

        # print('Writing to Parquet with DuckDB')
        #
        # # Save as parquet with duckdb
        # duckdb.sql(f"""
        #      copy(select * from read_json_auto({filename}.json))
        #      to '{filename}.parquet'
        #      (format parquet);
        #  """)

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

    regular_season_games = PythonOperator(
        task_id="reg_season_extract",
        python_callable=extract_from_api,
        op_kwargs={
            'url': _NHL_SEASONS_URL,
            'date': f"{now('America/Denver').year - 1}",
            'season_type': 'REG',
            'endpoint': 'schedule'
        }
    )

    post_season_games = PythonOperator(
        task_id="pst_season_extract",
        python_callable=extract_from_api,
        op_kwargs={
            'url': _NHL_SEASONS_URL,
            'date': f"{now('America/Denver').year - 1}",
            'season_type': 'PST',
            'endpoint': 'schedule'
        }
    )

    load_reg_season_json_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_reg_season_json_to_s3",
        filename=f'nhl_api_{now('America/Denver').year - 1}_extract_REG_schedule.json',
        dest_bucket="nhl-data-raw",
        dest_key=f"json/regular_season/nhl_api_{now('America/Denver').year - 1}_extract_REG_schedule.json",
        aws_conn_id = 's3_key',
        replace=True
    )

    load_playoff_season_json_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_playoff_season_json_to_s3",
        filename=f'nhl_api_{now('America/Denver').year - 1}_extract_PST_schedule.parquet',
        dest_bucket="nhl-data-raw",
        dest_key=f"json/post_season/nhl_api_{now('America/Denver').year - 1}_extract_PST_schedule.json",
        aws_conn_id = 's3_key',
        replace=True
    )

    # load_reg_season_parquet_to_s3 = LocalFilesystemToS3Operator(
    #     task_id="load_reg_season_parquet_to_s3",
    #     filename=f'nhl_api_{now('America/Denver').year - 1}_extract_REG_schedule.json',
    #     dest_bucket="nhl-data-raw",
    #     dest_key=f"parquet/regular_season/nhl_api_{now('America/Denver').year - 1}_extract_REG_schedule.parquet",
    #     aws_conn_id = 's3_key',
    #     replace=True
    # )

    # load_playoff_season_parquet_to_s3 = LocalFilesystemToS3Operator(
    #     task_id="load_playoff_season_parquet_to_s3",
    #     filename=f'nhl_api_{now('America/Denver').year - 1}_extract_PST_schedule.parquet',
    #     dest_bucket="nhl-data-raw",
    #     dest_key=f"parquet/post_season/nhl_api_{now('America/Denver').year - 1}_extract_PST_schedule.parquet",
    #     aws_conn_id = 's3_key',
    #     replace=True
    # )

    chain(
        regular_season_games,
        post_season_games,
        load_reg_season_json_to_s3,
        load_playoff_season_json_to_s3
        # load_reg_season_parquet_to_s3,
        # load_playoff_season_parquet_to_s3
    )

task_run()

