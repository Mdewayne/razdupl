
def extract_last_price(data):
    marketdata = data[1].get("marketdata", [])
    for item in marketdata:
        if item.get("BOARDID") == "TQBR":
            return item.get("LAST")
    raise ValueError("TQBR price not found")
