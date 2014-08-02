import pika
import json
import logging
import sys

from api.exceptions import StopConsuming

from api.settings import RABBITMQ_HOST
from api.settings import RABBITMQ_PORT
from api.settings import RABBITMQ_URL_QUEUE
from api.settings import LOG_LEVEL

logging.basicConfig()
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('pika.channel').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

class Producer(object):

    TIMEOUT = False

    def __init__(self, queue=None, callback=None):
        logger.debug("Connecting...")
        self.__cfg = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat_interval=2)
        self.__conn = pika.BlockingConnection(self.__cfg)
        logger.debug("Connected.")
        logger.debug("Opening channel...")
        self.__channel = self.__conn.channel()
        self.__queue = queue

        logger.debug("Setting confirm delivery")
        self.__channel.confirm_delivery()

        if Producer.TIMEOUT:
            logger.debug("Setting timeout callback")
            self.__conn.add_timeout(Producer.TIMEOUT, self.ontimeout)

    def ontimeout(self):
        pass

    def send(self, message):
        """
        Sends a json serializeable python object

        :param object message: The message to queue.
        """
        return self.__channel.basic_publish(exchange='',
                                            routing_key=RABBITMQ_URL_QUEUE,
                                            body=json.dumps(message),
                                            properties=pika.BasicProperties(
                                                delivery_mode=2
                                            ))


class Consumer(object):

    TIMEOUT = False

    def __init__(self, queue=None, callback=None):
        logger.debug("Consumer Connecting...")
        self.__cfg = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, heartbeat_interval=2)
        self.__conn = pika.BlockingConnection(self.__cfg)
        logger.debug("Connected..")
        logger.debug("Opening channel")
        self.__channel = self.__conn.channel()
        self.__callback = callback
        self.__queue = queue

        if Consumer.TIMEOUT:
            self.__conn.add_timeout(Consumer.TIMEOUT, self.stop)

        def f(ch, method, properties, body):
            logger.debug("Executing receive callback...")
            logger.debug(u"{0}".format(body))
            msg = json.loads(body)
            if isinstance(msg, (dict)):
                logger.debug("Message is a dict")
            elif isinstance(msg, (unicode, str)):
                logger.debug("Message is a string")
            if callback(msg):
                logger.debug("acking")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logger.debug("NOT acking")
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

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
