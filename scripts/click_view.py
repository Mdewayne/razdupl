import logging
from moex.db.run_click import execute_clickhouse_sql
from moex.config import (
    CLICKHOUSE_TABLE,
    CLICKHOUSE_TABLE_VIEW 
)

def update_view() -> None:
    """
    Обновляет витрину данных:
    1. Создает витрину, если она не существует
    2. Очищает витрину
    3. Вставляет последние данные из stage
    """
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE_VIEW} (
            symbol String,
            last_price Float64,
            ts DateTime
        ) ENGINE = MergeTree()
        ORDER BY ts
    """
    execute_clickhouse_sql(create_sql)
    logging.info(f"Ensured table {CLICKHOUSE_TABLE_VIEW} exists")

    # 2. Очищаем витрину
    truncate_sql = f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_TABLE_VIEW}"
    execute_clickhouse_sql(truncate_sql)
    logging.info(f"Truncated {CLICKHOUSE_TABLE_VIEW}")
    
    # 2. Вставляем последние данные
    insert_sql = f"""
        INSERT INTO {CLICKHOUSE_TABLE_VIEW}
        SELECT 
            symbol,
            last_price,
            ts
        FROM {CLICKHOUSE_TABLE}
        WHERE ts = (
            SELECT MAX(ts) 
            FROM {CLICKHOUSE_TABLE}
        )
    """
    execute_clickhouse_sql(insert_sql)
    logging.info(f"Inserted latest data into {CLICKHOUSE_TABLE_VIEW}")