import logging
import sys
import os 

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..') 

sys.path = [ROOT] + sys.path

from api.settings import RABBITMQ_URL_QUEUE
from api.settings import LOG_LEVEL
from api.services.mq import Consumer
from api import document

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

def process_message(msg):
    logger.debug("Processing message in callback...")

    if not msg:
        logger.debug("No message!")
        return False
    else:
        logger.debug(u"Message: {0}".format(msg))
        logger.debug("Got a message...")

    if isinstance(msg, (str, unicode)):
        logger.debug("Message is a string")
    if isinstance(msg, (dict)):
        logger.debug("Message is a dict")
    logger.debug("Getting list of urls...")
    urls = msg.get('urls')
    logger.debug("Got {0} urls".format(len(urls)))

    if not urls:
        logger.debug("No urls in message.")
        return False

    for url in urls:
        logger.debug(u"Looking up/downloading candidate: {0}".format(url))
        doc, errors = document.find_or_create(url)
        if errors:
            for err in errors:
                logger.warning(err)
    if doc:
        return True
    else:
        return False


if __name__ == '__main__':
    logger.debug("Consuming...")
    c = Consumer(callback=process_message, queue=RABBITMQ_URL_QUEUE)
    c.start()
