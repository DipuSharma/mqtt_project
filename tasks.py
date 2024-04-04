import time
from worker import celery_app

@celery_app.task()    
def publish_topic(client_id, topic, message, mongo_db):
    time.sleep(1)
    collection = mongo_db["mqtt_data"]
    data = {"client_id": client_id, "topic": topic, "message": message}
    collection.insert_one(data)
    return True