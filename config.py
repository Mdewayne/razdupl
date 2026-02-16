MOEX_STOCKS = [
    "SBER",
    "GAZP",
     "LKOH"
    ]

MOEX_CONFIG = {
    "data_dir": "/opt/airflow/data/moex/",
    "base_url": "https://iss.moex.com/iss/engines/stock/markets/shares/securities/",
    "params_static": {
        "iss.meta": "off",
        "iss.json": "extended",
        "marketdata.columns": "SECID,BOARDID,LAST",
    },
}

PG_CONN_ID = "pg_app"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_DB = "default"

POSTGRES_TABLE = "moex_prices"
CLICKHOUSE_TABLE = "moex_prices_click"
CLICKHOUSE_EXTERNAL_TABLE = "moex_prices_ext"
CLICKHOUSE_TABLE_VIEW = "moex_prices_view"