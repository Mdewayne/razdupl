from moex.config import PG_CONN_ID
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def run_sql(sql, params=None):

    logging.info(f"Running SQL: {sql}, params: {params}")
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(sql, parameters=params)
    logging.info(f"SQL executed successfully: {sql}, params: {params}")

