from moex.scripts.api import request_to_api
from moex.utils.extract_last_price import extract_last_price
from moex.utils.save_price import save_price


def fetch_data(ticker, **context):

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    data = request_to_api(ticker, data_interval_start, data_interval_end)

    if data is None:
        raise RuntimeError(f"Не удалось получить данные с MOEX для {ticker} (ответ None)")

    last_price = extract_last_price(data)

    save_price(ticker, last_price)