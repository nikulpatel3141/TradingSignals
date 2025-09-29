import json
import logging
import logging
import asyncio

from websockets.asyncio.client import connect, ClientConnection

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s")
logger = logging.getLogger(__name__)


WS_ENDPOINT = "wss://ws.kraken.com/v2"


def generate_subscribe_message(subscribe: bool) -> dict:
    return {
        "method": "subscribe" if subscribe else "unsubscribe",
        "params": {
            "channel": "book",
            "symbol": ["BTC/USD", "MATIC/GBP"],
        }
    }


async def delayed_unsubscribe(websocket: ClientConnection) -> dict:
    await asyncio.sleep(5)
    logging.info("Sending unsunscribe message")
    message = generate_subscribe_message(False)
    response = await websocket.send(json.dumps(message))
    logging.info(response)
    await asyncio.sleep(5)  # should see only heatbeats
    return response


async def main():
    subscribe_message = generate_subscribe_message(True)

    logging.info("Sending subscribe message")
    async with connect(WS_ENDPOINT) as websocket:
        sub_ack = await websocket.send(json.dumps(subscribe_message))
        logging.info(sub_ack)

        loop = asyncio.get_event_loop()
        unsub_result = loop.create_task(delayed_unsubscribe(websocket))

        async for message in websocket:
            logging.info(message)

            if unsub_result.done():
                break


if __name__ == "__main__":
    asyncio.run(main())

