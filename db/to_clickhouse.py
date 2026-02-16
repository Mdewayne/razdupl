import logging
from jinja2 import Template
import airflow.macros as macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from moex.db.run_click import execute_clickhouse_sql
from moex.config import (
    PG_CONN_ID,
    POSTGRES_TABLE,
    CLICKHOUSE_TABLE,
    CLICKHOUSE_EXTERNAL_TABLE
)

def setup_clickhouse_tables():
    """Создает все необходимые таблицы в ClickHouse"""

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    pg_conn = pg_hook.get_connection(PG_CONN_ID)
    
    sql_external = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_EXTERNAL_TABLE}
    ENGINE = PostgreSQL(
        '{pg_conn.host}:{pg_conn.port}',
        '{pg_conn.schema}',
        '{POSTGRES_TABLE}',
        '{pg_conn.login}',
        '{pg_conn.password}'
    );
    """
    result = execute_clickhouse_sql(sql_external)
    logging.info(f"SQL executed successfully: External table created - {result}")
    
    sql_target = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
        id UInt64,
        symbol String,
        last_price Float64,
        ts DateTime
    ) ENGINE = MergeTree()
    ORDER BY ts;
    """
    result = execute_clickhouse_sql(sql_target)
    logging.info(f"SQL executed successfully: Target table created - {result}")

def transfer_data_to_clickhouse(start_time, end_time):
    """Переносит данные из PostgreSQL в ClickHouse за указанный период"""
    
    template = Template("""
        INSERT INTO {{ clickhouse_table }}
        SELECT 
            id,
            symbol,
            toFloat64(last_price) as last_price,
            ts
        FROM {{ clickhouse_external_table }}
        WHERE ts BETWEEN toDateTime('{{ data_interval_start }}')
                 AND toDateTime(
                        {% if data_interval_start == data_interval_end %}
                            '{{ macros.datetime.now().strftime('%Y-%m-%d %H:%M:%S') }}'
                        {% else %}
                            '{{ data_interval_end }}'
                        {% endif %}
                    )
    """)

    rendered_sql = template.render(
        clickhouse_table=CLICKHOUSE_TABLE,
        clickhouse_external_table=CLICKHOUSE_EXTERNAL_TABLE,
        data_interval_start=start_time,
        data_interval_end=end_time,
        macros=macros
    )
    
    result = execute_clickhouse_sql(rendered_sql)
    logging.info(f"SQL executed successfully: Data after {start_time} transferred - {result}")

def check_transfer_result(start_time, end_time):
    """Проверяет сколько данных загружено за период"""
    
    sql_check = f"""
    SELECT 
        count() as records,
        min(ts) as min_date,
        max(ts) as max_date,
        groupUniqArray(symbol) as symbols
    FROM {CLICKHOUSE_TABLE}
    WHERE ts BETWEEN toDateTime('{start_time}') AND toDateTime('{end_time}');
    """
    
    result = execute_clickhouse_sql(sql_check)
    logging.info(f"SQL executed successfully: Transfer check completed - {result}")

def transfer_to_clickhouse(**context):
    """Основная функция для переноса данных в ClickHouse"""

    start_time = context["data_interval_start"].strftime('%Y-%m-%d %H:%M:%S')
    end_time = context["data_interval_end"].strftime('%Y-%m-%d %H:%M:%S')

    setup_clickhouse_tables()
    transfer_data_to_clickhouse(start_time, end_time)
    check_transfer_result(start_time, end_time)