FROM quay.io/astronomer/astro-runtime:12.7.1

RUN python -m venv dbtvenv && source dbtvenv/bin/activate && pip install --no-cache-dir dbt-snowflake && deactivate
