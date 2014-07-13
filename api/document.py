import datetime
import logging
import time

from api.services.db import document as db_document
from api.services import http
from api.services import mq
from api.models import Document

from api import canonicalize
from api import Gatherer

logger = logging.getLogger(__name__)

def find_or_create(url):
    """
    Checks the database to see if a recent api.models.Document object was recently created. If it was do nothing. Otherwise download the URL and create a new api.models.Document object.

    :param str url: The url for the document

    :rtype: tuple( api.models.Document, list( str ) )
    :returns: A document representing the processed url and a list of errors, if any
    """
    g = Gatherer()
    p = mq.Producer()

    canonical = canonicalize(url)
    doc = g.call(db_document.find_by_url(url=canonical))
    if not doc:
        markup = http.get_markup(url)
        type = http.get_type(markup)
        base = http.base_from_url(url)
        if type == 'rss':
            links = http.links_from_rss(markup)
        elif type == 'sgml':
            links = http.links_from_sgml(markup)

        hrefs = http.hrefs_from_links(base, links)

        for link, freq in hrefs:
            logger.debug("Queuing {0}...".format(link))
            success = p.send({'url': link})
            if not success:
                while not success:
                    time.sleep(1)
                    success = p.send({'url': link})

        d = Document(url=canonical,
                     markup=markup,
                     type=type,
                     updated_at=int(time.mktime(datetime.datetime.utcnow().timetuple())),
                     hrefs=hrefs)
        doc = g.call(db_document.create(d))

    return doc, g.errors


