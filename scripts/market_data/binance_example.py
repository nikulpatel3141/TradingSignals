
# Python Example for subscribing to a channel
import time
import json
import websocket
import threading
from datetime import datetime


CHANNEL_NAMES = {
    "level2": "level2",
    "user": "user",
    "tickers": "ticker",
    "ticker_batch": "ticker_batch",
    "status": "status",
    "market_trades": "market_trades",
    "candles": "candles",
}

WS_API_URL = "wss://advanced-trade-ws.coinbase.com"


def on_message(ws, message):
    data = json.loads(message)
    with open("Output1.txt", "a") as f:
        f.write(json.dumps(data) + "\n")

def subscribe_to_products(ws, products, channel_name):
    message = {
        "type": "subscribe",
        "channel": channel_name,
        "product_ids": products
    }
    ws.send(json.dumps(message))

def unsubscribe_to_products(ws, products, channel_name):
    message = {
        "type": "unsubscribe",
        "channel": channel_name,
        "product_ids": products
    }
    ws.send(json.dumps(message))

def on_open(ws):
    products = ["BTC-USD"]
    subscribe_to_products(ws, products, CHANNEL_NAMES["level2"])

def start_websocket():
    ws = websocket.WebSocketApp(WS_API_URL, on_open=on_open, on_message=on_message)
    ws.run_forever()

def main():
    ws_thread = threading.Thread(target=start_websocket)
    ws_thread.start()

    sent_unsub = False
    start_time = datetime.utcnow()

    try:
        while True:
            if (datetime.utcnow() - start_time).total_seconds() > 5 and not sent_unsub:
                # Unsubscribe after 5 seconds
                ws = websocket.create_connection(WS_API_URL)
                unsubscribe_to_products(ws, ["BTC-USD"], CHANNEL_NAMES["level2"])
                ws.close()
                sent_unsub = True
            time.sleep(1)
    except Exception as e:
        print(f"Exception: {e}")


if __name__ == "__main__":
    main()
