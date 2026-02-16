from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from moex.scripts.manager import fetch_data
from moex.db.create_table import create_table_if_not_exists
from moex.db.to_clickhouse import transfer_to_clickhouse
from moex.config import MOEX_STOCKS
from airflow.datasets import Dataset

moex_dataset = Dataset("moex_loader_done")

with DAG(
    "moex_data_loader",
    description="MOEX stock price loader",
    schedule="0 12 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["moex", "stocks"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end",
        outlets=[moex_dataset])

    create_table = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    fetch_tasks = []
    for ticker in MOEX_STOCKS:
        fetch_task = PythonOperator(
            task_id=f"fetch_{ticker.lower()}",
            python_callable=fetch_data,
            op_kwargs={"ticker": ticker},
        )
        fetch_tasks.append(fetch_task)

    transfer_to_clickhouse = PythonOperator(
        task_id='transfer_to_clickhouse',
        python_callable=transfer_to_clickhouse,
    )

    start >> create_table >> fetch_tasks >> transfer_to_clickhouse >> end
