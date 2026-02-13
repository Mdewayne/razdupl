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