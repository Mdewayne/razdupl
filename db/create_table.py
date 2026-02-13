import logging
from airflow.models.taskinstance import TaskInstance
from moex.db.run_sql import run_sql

def create_table_if_not_exists():

    logging.info("Создаём таблицу moex_prices, если её ещё нет")
    run_sql(
        """
        CREATE TABLE IF NOT EXISTS moex_prices (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            last_price NUMERIC NOT NULL,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )

