from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from moex.scripts.manager import fetch_data
from moex.scripts.db import store_prices_to_postgres
from moex.config import MOEX_STOCKS


with DAG(
    "moex_data_loader",
    description="MOEX stock price loader",
    schedule="0 12 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["moex", "stocks"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Параллельно фетчим цены по тикерам
    fetch_tasks = []
    for ticker in MOEX_STOCKS:
        fetch_task = PythonOperator(
            task_id=f"fetch_{ticker.lower()}",
            python_callable=fetch_data,
            op_kwargs={"ticker": ticker},
        )
        fetch_tasks.append(fetch_task)

    # 2. После всех фетчей – одним шагом пишем в Postgres
    store = PythonOperator(
        task_id="store_prices_to_postgres",
        python_callable=store_prices_to_postgres,
    )

    start >> fetch_tasks >> store >> end





