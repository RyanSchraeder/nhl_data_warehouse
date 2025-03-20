# NHL Data Warehouse Project 

> ![NHLDataPipeline drawio](https://github.com/user-attachments/assets/92d5aa6a-68ef-4ff0-ab0a-4f2d886fa8ef)


Overview
========

#### Description
A data pipeline that extracts, loads, and transforms (ELT) data from [hockeyreference](https://hockeyreference.com) into game and teams statistics models within Snowflake. 

#### Process
Using Astronomer, deploys a Docker container in Apache Airflow. In this environment, data is scraped from https://hockeyreference.com for regular season data and team-level statistics. The data is normalized to a columnar format, then loaded into Snowflake. From there, the data is transformed with dbt models, all orchestrated by Airflow!

#### Data Pipeline Tech: 
- Python, SQL
- Docker
- Apache Airflow
- dbt
- Astronomer
- Snowflake

### Usage
- Install astro with `python -m pip install astro`
- Run `astro init` to initialize the Astronomer Astro CLI project
- Start up the docker engine. You can do this with Docker CLI or Docker Desktop. Make sure you have Docker installed locally!
- Run `astro dev start` to build and host Airflow on your localhost (default to port 8080)
- Log into Airflow at http://localhost:8080
- Get building!
