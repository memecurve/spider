import datetime
import logging
import time

from api.services.db import document as db_document
from api.services import http
from api.services import mq
from api.models import Document
from api.settings import LOG_LEVEL

from api import canonicalize

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.propagate = True
logger.setLevel(LOG_LEVEL)

def find_or_create(url):
    """
    Checks the database to see if a recent api.models.Document object was recently created. If it was do nothing. Otherwise download the URL and create a new api.models.Document object.

    :param str url: The url for the document

    :rtype: tuple( api.models.Document, list( str ) )
    :returns: A document representing the processed url and a list of errors, if any
    """
    p = mq.Producer()

    canonical = canonicalize(url)
    logger.debug("Canonical url: {0}".format(canonical))
    doc = db_document.find_by_url(url=canonical)
    logger.debug("Doc: {0}".format(doc))
    if not doc:
        logger.debug("fetching markup...")
        markup = http.get_markup(url)
        logger.debug("Detecting type...")
        type = http.get_type(markup)
        logger.debug("{0}".format(type))
        base = http.base_from_url(url)
        logger.debug("Base: {0}".format(base))
        if type == 'rss':
            links = http.links_from_rss(markup)
        elif type == 'sgml':
            links = http.links_from_sgml(markup)
        logger.debug("Found {0} links...".format(len(links)))

        hrefs = http.hrefs_from_links(base, links)

        to_queue = []
        for link, freq in hrefs:
            to_queue.append(link)
            logger.debug("Queuing {0}...".format(link))

        logger.debug("Sending...")
        while not p.send({'urls': to_queue}):
            logger.debug("Failed. Sleeping...")
            time.sleep(1)

        d = Document(url=canonical,
                     markup=markup,
                     type=type,
                     updated_at=int(time.mktime(datetime.datetime.utcnow().timetuple())),
                     hrefs=hrefs)
        logger.debug("Creating database document...")
        doc = db_document.create(d)

    return doc 


