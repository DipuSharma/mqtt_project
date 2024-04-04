from fastapi import FastAPI, Depends
from pymongo import MongoClient
from paho.mqtt import client as mqtt_client
from worker import celery_app
from tasks import publish_topic
import uvicorn
import random
broker = 'broker.emqx.io'
port = 1883
topic = "/python/mqtt"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

app = FastAPI()

# Dependency for MongoDB connection
def get_mongo_client():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["mydatabase"]
    return db


# MQTT endpoint
@app.post("/mqtt")
async def receive_mqtt_message(topic: str, message: str, mongo_db=Depends(get_mongo_client)):
    publish_topic.delay(topic, message, mongo_db)
    return {"status": "Your Message received and proccessing in background"}

@app.get("/mongodb/")
async def get_mongodb_data(mongo_db=Depends(get_mongo_client)):
    collection = mongo_db["mqtt_data"]
    results = collection.find()
    data = [{
        "id": str(item['_id']),
        "topic": item["topic"],
        "message": item["message"]
    } for item in results]
    return {"data": data}


def connect_mqtt():
    client = mqtt_client.Client(client_id)
    client.connect(broker, port)
    return client


if __name__ == "__main__":
    client = connect_mqtt()
    client.loop_start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
