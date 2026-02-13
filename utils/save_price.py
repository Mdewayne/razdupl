from moex.config import MOEX_CONFIG
from moex.db.run_sql import run_sql

def save_price(ticker, last_price):

    run_sql(
        """
        INSERT INTO moex_prices (symbol, last_price) 
        VALUES (%s, %s)
        """,
        (ticker, last_price)
    )
