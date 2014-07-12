import pika
import json
import logging
import sys

from api.exceptions import StopConsuming

from api.settings import RABBITMQ_HOST
from api.settings import RABBITMQ_PORT
from api.settings import RABBITMQ_URL_QUEUE


logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('pika.channel').setLevel(logging.WARNING)

def queue_for_downloading(url):
    """
    Sends a url to the queue to be downloaded.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
    channel = connection.channel()
    channel.confirm_delivery()
    channel.basic_publish(exchange='',
                          routing_key=RABBITMQ_URL_QUEUE,
                          body=json.dumps({'url': url}),
                          properties=pika.BasicProperties(
                              delivery_mode=2
                          ))
    connection.close()

class Consumer(object):

    TIMEOUT = False

    def __init__(self, queue=None, callback=None):
        self.__cfg = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        self.__conn = pika.BlockingConnection(self.__cfg)
        self.__channel = self.__conn.channel()
        self.__callback = callback
        self.__queue = queue

        if Consumer.TIMEOUT:
            self.__conn.add_timeout(Consumer.TIMEOUT, self.stop)

        def f(ch, method, properties, body):
            if callback(json.loads(body)):
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.__f = f

    def consume_one(self):
        self.__channel.basic_consume(self.__f,
                                     queue=self.__queue,
                                     no_ack=False)

    def start(self):
        self.consume_one()
        self.__channel.start_consuming()

    def stop(self):
        self.__channel.stop_consuming()

def pass_urls(callback, daemonize=None):
    """
    Consumes from the URL queue, passing message to `callback`

    :param callable callback: A function that processes messages from the URL queue. Should return True on success and False otherwise.
    """
    c = Consumer(queue=RABBITMQ_URL_QUEUE,
                 callback=callback)
    if daemonize:
        c.start()
    else:
        c.consume_one()
