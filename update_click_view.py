from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import pendulum
from moex.scripts.click_view import update_view
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.datasets import Dataset


moex_dataset = Dataset("moex_loader_done")

with DAG(
    dag_id="moex_view_update",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=[moex_dataset],
    catchup=False,
    tags=["moex", "view"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    update_view_task = PythonOperator(
        task_id="update_view",
        python_callable=update_view,
    )

    start >> update_view_task >> end