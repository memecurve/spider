import logging
import sys
import os 

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..') 
print ROOT

sys.path = [ROOT] + sys.path

from api.settings import RABBITMQ_URL_QUEUE
from api.services.mq import Consumer
from api import document

logger = logging.getLogger(__name__)

def process_message(msg):
    if not msg: return None
    url = msg.get('url', None)
    if not url: return None

    doc, errors = document.find_or_create(url)
    if errors:
        for err in errors:
            logger.warning(err)


if __name__ == '__main__':
    c = Consumer(callback=process_message, queue=RABBITMQ_URL_QUEUE)
    c.start()
