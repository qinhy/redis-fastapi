import asyncio
import json
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI()

redis_client = redis.Redis(host='localhost', decode_responses=True)  # Redis client connection


@app.get("/", response_class=HTMLResponse)
async def get_homepage():
    with open('./gui.html','r') as f:
        return f.read()
    return "Not found gui.html"

@app.websocket("/redis/ws/pubsub/{channel}")
async def websocket_endpoint(websocket: WebSocket, channel: str):
    await websocket.accept()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)  # Subscribe to the Redis channel using the parameter
    
    async def send_messages():
        async for message in pubsub.listen():
            if message["type"] == "message":
                await websocket.send_text(message["data"])
    
    # Create a background task for sending messages
    send_task = asyncio.create_task(send_messages())
    
    try:
        while True:
            data = await websocket.receive_text()
            await redis_client.publish(channel, data)  # Publish messages to Redis using the parameter
    except Exception as e:
        print(f"WebSocket connection closed: {e}")
    finally:
        send_task.cancel()  # Clean up the background task when the connection is closed
        await pubsub.unsubscribe(channel)

@app.websocket("/redis/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            # Receive data from the WebSocket client
            data = await websocket.receive_text()
            
            # Parse incoming data (expecting JSON-formatted string)
            try:
                command_data:dict = json.loads(data)
                command = command_data.get("command")
                key = command_data.get("key")
                value = command_data.get("value")
            except json.JSONDecodeError:
                await websocket.send_text("Invalid input. Expected JSON formatted data.")
                continue

            if command == "SET" and key and value:
                # Set key-value pair in Redis
                await redis_client.set(key, value)
                await websocket.send_text(f"SET: {key} = {value}")

            elif command == "GET" and key:
                # Get value for a key from Redis
                result = await redis_client.get(key)
                if result is None:
                    await websocket.send_text(f"GET: {key} does not exist")
                else:
                    await websocket.send_text(f"GET: {key} = {result}")

            elif command == "DEL" and key:
                # Delete a key from Redis
                deleted_count = await redis_client.delete(key)
                if deleted_count > 0:
                    await websocket.send_text(f"DEL: {key} deleted")
                else:
                    await websocket.send_text(f"DEL: {key} does not exist")

            elif command == "KEYS":
                # Get all keys matching a pattern (default pattern is '*')
                pattern = key if key else '*'
                keys = await redis_client.keys(pattern)
                await websocket.send_text(f"KEYS: {keys}")

            else:
                # Unsupported command or missing parameters
                await websocket.send_text("Unsupported command or missing parameters. Use SET, GET, DEL, KEYS.")
    
    except Exception as e:
        print(f"WebSocket connection closed: {e}")

@app.post("/redis/set/{key}")
async def set_key(key: str, data: dict=dict(data='NULL')):
    if not data.get('data'):
        raise HTTPException(status_code=400, detail="Key and value must be provided")
    await redis_client.set(key, data.get('data'))
    return {"message": f"Key '{key}' set to '{data.get('data')}'"}

@app.get("/redis/get/{key}")
async def get_key(key: str):
    value = await redis_client.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"key": key, "value": value}

@app.delete("/redis/delete/{key}")
async def delete_key(key: str):
    deleted_count = await redis_client.delete(key)
    if deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"message": f"Key '{key}' deleted"}

@app.get("/redis/keys/{pattern}")
async def get_keys(pattern: str = "*"):
    keys = await redis_client.keys(pattern)
    if not keys:
        return {"message": "No keys found"}
    return {"keys": keys}

@app.post("/redis/pub/{channel}")
async def publish_message(channel: str, msg: dict=dict(data='NULL')):
    await redis_client.publish(channel, msg['data'])
    return {"message": f"Message published to channel '{channel}'"}

@app.get("/redis/sub/{channel}")
async def subscribe_to_channel(channel: str):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)

    async def event_stream():
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield f"data: {message['data']}\n\n"  # Ensure double newline
        except asyncio.CancelledError:
            print(f"Stopping subscription to channel '{channel}'")
        finally:
            await pubsub.unsubscribe(channel)

    return StreamingResponse(event_stream(), media_type="text/event-stream")