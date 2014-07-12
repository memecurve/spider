from fabric.api import task
import pika

from api.settings import *

@task
def create_queues():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_URL_QUEUE, durable=True)

