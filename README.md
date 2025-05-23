# NHL Data Warehouse Project 

> ![NHLDataPipeline drawio](https://github.com/user-attachments/assets/92d5aa6a-68ef-4ff0-ab0a-4f2d886fa8ef)


Overview
========

#### Description
A data pipeline that extracts, loads, and transforms (ELT) data from [hockeyreference](https://hockeyreference.com) into game and teams statistics models within Snowflake. 

#### Visualization 
Check out the Streamlit app! [![NHL Season Overview](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://nhlseasonoverview.streamlit.app)
#### Process
Using Astronomer, deploys Apache Airflow and all project dependencies in a Docker container. In this environment, data is scraped from https://hockeyreference.com for regular season data and team-level statistics. The data is normalized to a columnar format (.csv file) then loaded into Amazon S3. Using a storage integration in Snowflake, Amazon S3 is synchronized for file updates and data is loaded into Snowflake. From there, the data is transformed with dbt models, all orchestrated by Airflow!

![image](https://github.com/user-attachments/assets/66e22912-1246-496e-b998-25b3715e90c0)

#### Data Pipeline Tech: 
- Python, SQL
- Docker
- Apache Airflow
- dbt (raw, staging, and mart layers or medallions)
- Astronomer
- Snowflake

### Usage
- Install astro with `python -m pip install astro`
- Run `astro init` to initialize the Astronomer Astro CLI project
- Start up the docker engine. You can do this with Docker CLI or Docker Desktop. Make sure you have Docker installed locally!
- Run `astro dev start` to build and host Airflow on your localhost (default to port 8080)
- Log into Airflow at http://localhost:8080
- Get building!

## Pending Ideas 
- Use the SportsRadar NHL API to pull additional info on players, game summary statistics on the team level, and playoff schedule
