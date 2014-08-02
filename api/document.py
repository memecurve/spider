import datetime
import logging
import time

from api.services.db import document as db_document
from api.services.db.word_count import WordCount
from api.services.db import HbaseInternals
from api.services import http
from api.services import mq
from api.models import Document
from api.exceptions import SpiderException

from api.settings import LOG_LEVEL
from api.settings import DISCOVER_NEW
from api.settings import CYCLE_RESOLUTION

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
    doc = db_document.find_by_url(url=canonical)
    if not doc:
        markup = http.get_markup(url)
        type = http.get_type(markup)
        base = http.base_from_url(url)
        if type == 'rss':
            links = http.links_from_rss(markup)
        elif type == 'sgml':
            links = http.links_from_sgml(markup)
            word_counts = http.wordcounts_from_sgml(markup)
            WordCount.store(word_counts)

        hrefs = http.hrefs_from_links(base, links)

        to_queue = []
        for link, freq in hrefs:
            to_queue.append(link)

        if DISCOVER_NEW:
            for i in xrange(int(len(to_queue)/50) + 1):
                while not p.send({'urls': to_queue[i*50:(i+1)*50]}):
                    logger.warning("Failed to queue messages. Sleeping...")
                    time.sleep(1)

        d = Document(url=canonical,
                     markup=markup,
                     type=type,
                     updated_at=int(time.mktime(datetime.datetime.utcnow().timetuple())),
                     hrefs=hrefs)

        doc = db_document.create(d)

    return doc 

def find_by_unix_time(since=None, until=None, type=None, limit=None):
    i = HbaseInternals()

    if since is None:
        since = i.get_timestamp() - (2*CYCLE_RESOLUTION) # Everything since the last run
    if until is None:
        until = i.get_timestamp()

    return db_document.find(updated_at__lte=until, updated_at__gte=since, limit=limit, type=type)
