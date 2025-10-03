"""
https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview
"""
import asyncio
import time
import json
from typing import Final

from websockets.asyncio.client import connect

BINANCE_ENDPOINT: Final[str] = "wss://advanced-trade-ws.coinbase.com"

def subscribe_message(subscribe: bool, products: list[str], channel: str) -> dict:
    return {
        "type": "subscribe" if subscribe else "unsubscribe",
        "product_ids": products,
        "channel": channel,
    }


async def main():
    products = ["ETH-USD", "BTC-USD"]
    channel = "level2"

    def subscribe_message_(subscribe):
        return json.dumps(subscribe_message(subscribe, products, channel))

    start_time = time.time()

    async with connect(BINANCE_ENDPOINT) as websocket_listener:
        print("Sending subscribe request")
        subscribe_response = await websocket_listener.send(subscribe_message_(True))
        print(subscribe_response)

        async for message in websocket_listener:
            print(message)
            if (time.time() - start_time) > 10.0:
                print("Sending unsubscribe request")
                unsubscribe_response = await websocket_listener.send(subscribe_message_(False))
                print(unsubscribe_response)
        

if __name__ == "__main__":
    asyncio.run(main())
