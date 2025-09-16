import asyncio
import json

from websockets.asyncio.server import serve

_message_count = 0

async def handler(websocket):
    global _message_count
    
    while True:
        print(f"Sending message {_message_count}")
        event = {"payload": {"count": _message_count}}
        _message_count += 1
        await websocket.send(json.dumps(event))
        await asyncio.sleep(1)
        

async def main():
    async with serve(handler, "", 8001) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
