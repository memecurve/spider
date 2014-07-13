import sys

from api import canonicalize
from api import Gatherer

from api.services.db import Internals

from api.models import Document

TABLE_NAME = 'document'

def create(document, created_at=None):
    """
    Saves a document to the database.

    :param :class:`api.models.Document` document: The document to create.

    :rtype: api.models.Document, list( str )
    :returns: The document saved to the database and a list of errors, if any.
    """
    internals = Internals()
    updated_at = created_at or internals.get_timestamp()
    hrefs = {'href:{0}'.format(ref): str(freq) for ref, freq in document.hrefs}
    doc = { 'self:url': document.url,
            'self:type': document.type,
            'self:updated_at': str(updated_at),
            'data:markup': document.markup }
    doc.update(hrefs)

    row_key ="{0}{1}{2}".format(document.type,
                                updated_at,
                                document.url)

    _, errors = internals.write(table=TABLE_NAME,
                                row_key=row_key, doc=doc)

    document.updated_at = updated_at

    return document, errors

def _get_row_range(type=None, updated_at__lte=None, updated_at__gte=None, url=None):
    """
    Returns start and end row prefixes for the parameters passed.

    :param str type: Either rss or sgml. Useful for efficiently seeding. Defaults to 'rss'
    :param str updated_at__lte: Timestamps for the records found will be less than or equal to this value. Defaults to the current time.
    :param str updated_at__gte: Timestamps for the records found will be greater than or equal to this value. Defaults to 7 days prior to the updated_at__lte
    :param str url: The canonical url to look up.

    :rtype: tuple( str, str )
    :returns: The starting and ending row keys for the range specified.
    """
    internals = Internals()

    if type not in ('rss', 'sgml'):
        raise ValueError("`type` must be one of 'rss' or 'sgml'")
    if updated_at__lte is None:
        updated_at__lte = internals.get_timestamp()
    if updated_at__gte is None:
        updated_at__gte = updated_at__lte - 604800
    if url is None:
        url = ''
    else:
        url = canonicalize(url)

    start_row = "{0}{1}{2}".format(type, updated_at__gte, url)
    end_row = "{0}{1}{2}".format(type, updated_at__lte, url)

    return start_row, end_row

def find_by_url(url=None, updated_at__lte=None, updated_at__gte=None, include_links=False, include_markup=False):
    """
    Finds a document matching url of either type, 'rss' or 'sgml'.

    :param str url: The url to look up.
    :param str updated_at__lte: Timestamps for the records found will be less than or equal to this value. Defaults to the current time.
    :param str updated_at__gte: Timestamps for the records found will be greater than or equal to this value. Defaults to 7 days prior to the updated_at__lte
    :param bool include_links: Set this equal to True to include link frequencies stored with the query results. Defaults to False.
    :param bool include_markup: Set this equal to True to include the markup for the document in the query results. Defaults to False.
    """
    columns = ['self']
    if include_markup:
        columns.append('data')
    if include_links:
        columns.append('href')

    start_row, end_row = _get_row_range(type='sgml', url=url,
                                        updated_at__lte=updated_at__lte, updated_at__gte=updated_at__gte)

    internals = Internals()
    g = Gatherer()

    docs = [d for d in g.call(internals.find(table=TABLE_NAME,
                                             row_start=start_row, row_stop=end_row,
                                             columns=columns, filter="SingleColumnValueFilter ('self','url',=,'substring:{0}')".format(url),
                                             limit=1))]

    if not docs:
        start_row, end_row = _get_row_range(type='rss', url=url,
                                            updated_at__lte=updated_at__lte, updated_at__gte=updated_at__gte)
        docs = g.call(internals.find(table=TABLE_NAME,
                                     row_start=start_row, row_stop=end_row,
                                     columns=columns, filter="SingleColumnValueFilter ('self','url',=,'substring:{0}')".format(url),
                                     limit=1))

    docs = [g.call(hbase_to_model(d)) for d in docs]
    if docs:
        return docs[0], g.errors
    return None, g.errors

def find(type=None, updated_at__lte=None, updated_at__gte=None, include_links=False, include_markup=False, limit=None):
    """
    Finds documents matching url, type, or a range for created_at/updated_at. By default only scans documents from the last 7 days..

    :param str type: Either rss or sgml. Useful for efficiently seeding. Defaults to 'rss'
    :param str updated_at__lte: Timestamps for the records found will be less than or equal to this value. Defaults to the current time.
    :param str updated_at__gte: Timestamps for the records found will be greater than or equal to this value. Defaults to 7 days prior to the updated_at__lte
    :param bool include_links: Set this equal to True to include link frequencies stored with the query results. Defaults to False.
    :param bool include_markup: Set this equal to True to include the markup for the document in the query results. Defaults to False.
    :param int limit: The maximum number of results to return

    :returns: A list of documents matching the input criteria and a list of errors, if any.
    :rtype: tuple( list( :class:`api.models.Document` ), list( str ) )
    """
    columns = ['self']
    if include_markup:
        columns.append('data')
    if include_links:
        columns.append('href')

    g = Gatherer()
    internals = Internals()

    start_row, end_row = _get_row_range(type=type, updated_at__lte=updated_at__lte, updated_at__gte=updated_at__gte)
    docs = g.call(internals.find(table=TABLE_NAME,
                                 row_start=start_row, row_stop=end_row,
                                 columns=columns, limit=limit))

    return [g.call(hbase_to_model(d)) for d in docs], g.errors

def hbase_to_model(d):
    """
    (
        'sgml1405193677http://www.buzzfeed.com',
        {
            'self:type': ('sgml', 1405190077088),
            'href:http://www.facebook.com/': ('10', 1405190077088),
            'data:markup': ('<html></html>', 1405190077088),
            'href:http://www.google.com/': ('1', 1405190077088),
            'self:url': ('http://www.buzzfeed.com', 1405190077088),
            'self:updated_at': ('1405193677', 1405190077088),
            'href:http://www.yahoo.com/': ('5', 1405190077088)
        }
    )
    """
    row_key, doc = d
    params = {}
    hrefs = []
    try:
        for k, v in doc.items():
            split_key = k.split(':')
            if split_key[1] == 'updated_at':
                params[":".join(split_key[1:])] = int(v[0])
            elif split_key[0] != 'href':
                params[":".join(split_key[1:])] = v[0]
            else:
                hrefs.append((":".join(split_key[1:]), int(v[0])))
        params['hrefs'] = hrefs
    except (IndexError, KeyError), e:
        return None, ["Improperly formatted database result."]
    return Document(**params), []




