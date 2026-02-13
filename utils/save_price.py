from pathlib import Path
import json
import pendulum
from moex.config import MOEX_CONFIG

def save_price(ticker, last_price):

    data_dir = Path(MOEX_CONFIG["data_dir"])
    data_dir.mkdir(parents=True, exist_ok=True)

    timestamp = pendulum.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_file = data_dir / f"{ticker}_{timestamp}.json"

    try:
        with open(output_file, 'w') as f:
            json.dump({
                "symbol": ticker,
                "last_price": last_price,
                "timestamp": timestamp
            }, f, indent=2)
    except Exception as e:
        raise RuntimeError(f"Ошибка при сохранении {ticker}: {e}")
