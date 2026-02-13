from datetime import datetime
from moex.config import MOEX_CONFIG
import requests
import logging

def request_to_api(ticker: str, data_interval_start: datetime, data_interval_end: datetime):

    url = f"{MOEX_CONFIG['base_url']}{ticker}.json"

    params = MOEX_CONFIG["params_static"].copy()
    params.update({"from": data_interval_start, "to": data_interval_end})

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Ошибка запроса к MOEX для {ticker}: {e}")
        return None