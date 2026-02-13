from __future__ import annotations

from typing import Any, List, Sequence, Tuple
import logging
from airflow.models.taskinstance import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook

from moex.config import MOEX_STOCKS


TableRow = Tuple[str, float]


def _ensure_moex_prices_table(hook: PostgresHook) -> None:
    """
    Отвечает только за DDL: создать таблицу, если её нет.
    """
    logging.info("Создаём таблицу moex_prices, если её ещё нет")
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS moex_prices (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            last_price NUMERIC NOT NULL,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def _collect_prices_from_xcom(ti: TaskInstance) -> List[TableRow]:
    """
    Отвечает только за вытаскивание данных из XCom
    и приведение их к удобному формату для записи в БД.
    """
    rows: List[TableRow] = []

    for ticker in MOEX_STOCKS:
        task_id = f"fetch_{ticker.lower()}"
        result = ti.xcom_pull(task_ids=task_id)
        if not result:
            continue

        price = result.get("price")
        symbol = result.get("symbol", ticker)
        if price is None:
            continue

        rows.append((symbol, float(price)))

    logging.info(
        "Собрано %s цен из XCom для тикеров: %s",
        len(rows),
        ", ".join(symbol for symbol, _ in rows) or "-",
    )

    return rows


def _insert_prices(hook: PostgresHook, rows: Sequence[TableRow]) -> None:
    """
    Отвечает только за INSERT в Postgres.
    """
    if not rows:
        logging.info("Нет данных для вставки в moex_prices – пропускаем INSERT")
        return

    hook.insert_rows(
        table="moex_prices",
        rows=list(rows),
        target_fields=["symbol", "last_price"],
    )
    logging.info("Успешно вставлено %s строк в moex_prices", len(rows))


def store_prices_to_postgres(**context: Any) -> None:
    """
    Orchestrator-функция для PythonOperator:

    1) создаёт hook к Postgres;
    2) гарантирует наличие таблицы;
    3) собирает данные из XCom;
    4) вставляет их в Postgres.
    """
    ti: TaskInstance = context["ti"]
    hook = PostgresHook(postgres_conn_id="pg_app")

    logging.info("Старт задачи store_prices_to_postgres")
    _ensure_moex_prices_table(hook)
    rows = _collect_prices_from_xcom(ti)
    _insert_prices(hook, rows)
    logging.info("Завершение задачи store_prices_to_postgres")

