from fabric.api import task

from api.settings import RABBITMQ_URL_QUEUE
from api.services.mq import Producer, Consumer
from scripts.worker import process_message

@task
def worker():
    p = Producer()
    p.send({'urls': ['http://www.etalkinghead.com/directory']})
    c = Consumer(callback=process_message, queue=RABBITMQ_URL_QUEUE)
    c.start()
