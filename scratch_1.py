import json
import duckdb
import requests
from datetime import datetime
from pprint import pprint
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv()

_NHL_SEASONS_URL = "https://api.sportradar.com/nhl/trial/v7/en/games"
_API_KEY = os.getenv("API_KEY")


def extract(url, date, season_type, endpoint):
    try:
        if endpoint == "schedule":
            url = f"{_NHL_SEASONS_URL}/{date}/{season_type}/schedule.json?api_key={_API_KEY}"
            print(f'URL Built: {url}')

        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        content = response.json()


        print(content)

        filename = f'nhl_api_{date}_extract_{endpoint}'

        print(f'Filename prepared: {filename}')

        with open(f'{filename}.json', 'w') as file:
            json.dump(content, file, indent=4)

        print('Writing to Parquet with DuckDB')

        # Save as parquet with duckdb
        duckdb.sql(f"""
             copy(select * from read_json_auto({filename}.json))
             to '{filename}.parquet'
             (format parquet);
         """)

        return "Success"

    except Exception as e:
        raise print(f"Issue occurred extracting from the NHL API: {e}")


extract(
    url=_NHL_SEASONS_URL,
    date=f"{datetime.now().year - 1}",
    season_type='REG',
    endpoint='schedule'
)
