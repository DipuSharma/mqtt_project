from celery import Celery

celery_app = Celery(
    "worker",
    broker_url='amqp://guest:guest@localhost:5672/',
    result_backend='rpc://',
)