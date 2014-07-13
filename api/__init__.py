import urlparse
import urllib
import operator

def canonicalize(url):
    """
    Returns a canonical url from the input url (must be absolute, including protocol). Strips utm_term and fb_ref parameters. Also sorts parameters lexicographically and removes url hashes.

    :param str url: The url to canonicalize.
    """
    p = urlparse.urlparse(url)
    if not p.scheme:
        raise ValueError("Url must be absolute.")
    qs = urlparse.parse_qsl(p.query)
    canonical_qs = []
    for k, v in qs:
        if k.startswith('utm_') or k == 'fb_ref':
            continue
        else:
            canonical_qs.append((k, v))
    canonical_qs.sort(key=operator.itemgetter(0))
    if canonical_qs:
        return "{0}://{1}{2}?{3}".format(p.scheme, p.netloc, p.path or '/', urllib.urlencode(canonical_qs))
    return "{0}://{1}{2}".format(p.scheme, p.netloc, p.path or '/')

class Gatherer(object):

    def __init__(self):
        self.errors = []

    def call(self, r):
        result, errors = r
        errors += errors
        return result

