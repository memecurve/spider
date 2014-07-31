import logging
import time
import sys
import os 

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..') 

sys.path = [ROOT] + sys.path

from api.settings import RABBITMQ_URL_QUEUE
from api.settings import LOG_LEVEL
from api.settings import DISCOVER_NEW
from api.settings import MAX_DOWNLOADS

from api.services.mq import Consumer
from api.services.mq import Producer
from api import document

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

if not DISCOVER_NEW:
    docs = set([d.url for d in document.find_by_unix_time(limit=MAX_DOWNLOADS, type='rss')])
    if len(docs) < MAX_DOWNLOADS:
        docs = docs.union(set([d.url for d in document.find_by_unix_time(limit=MAX_DOWNLOADS - len(docs), type='sgml')]))
    docs = list(docs)
    for i in xrange(int(len(docs)/50) + 1):
        slce = [d for d in docs[i*50:(i+1)*50]]
        p = Producer()
        if slce:
            while not p.send({'urls': slce}):
                logger.warning("Failed to queue messages. Sleeping...")
                time.sleep(1)


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
        logger.warning("No urls in message.")
        return False

    for url in urls:
        logger.debug(u"Looking up/downloading candidate: {0}".format(url))
        doc = document.find_or_create(url)
    if doc:
        return True
    else:
        logger.warning(u"Didn't create a document for {0}".format(url))
        return False


if __name__ == '__main__':
    logger.debug("Consuming...")
    c = Consumer(callback=process_message, queue=RABBITMQ_URL_QUEUE)
    c.start()
