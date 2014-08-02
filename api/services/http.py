import sys
import re
import logging
from urlparse import urlparse
from collections import defaultdict

import feedparser
import requests
from bs4 import BeautifulSoup

from api import canonicalize
from api.settings import LOG_LEVEL
from api.settings import EXTENSION_BLACKLIST
from api.settings import ILLEGAL_CHARS
from api.settings import STOP_WORDS

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

ROOT_FEEDS = [
    "http://www.etalkinghead.com/directory", # Contains a lot of blogspot blogs
]

def get_markup(url):
    """
    Downloads the markup at a url.

    :param str url: The url for a resource to download

    :rtype: str
    :returns: A string representation of the markup
    """
    try:
        return requests.get(url).content
    except (requests.exceptions.ConnectionError,
            requests.exceptions.InvalidURL,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ChunkedEncodingError,
    ), e:
        return u''

def get_type(markup):
    """
    Returns the type of markup. Either SGML or RSS

    :param str markup: The markup for the document.

    :rtype: str
    :returns: One of 'rss' or 'sgml'
    """
    if not feedparser.parse(markup).version:
        return u'sgml'
    return u'rss'

def links_from_sgml(markup):
    """
    Given a string representation of a document (sgml) returns a list of all the links on the page, relative and absolute. JavaScript links omitted.

    :param str markup: A valid sgml document.

    :returns: A list of links as strings.
    :rtype: list( str )
    """
    bs4_links = BeautifulSoup(markup).find_all('a')
    str_links = []
    for link in bs4_links:
        href = link.get('href', '')
        u = urlparse(href)
        blacklisted = False
        for extension in EXTENSION_BLACKLIST:
            if u.path.endswith(extension):
                blacklisted = True
        if blacklisted:
            continue
        if u.scheme.lower() == 'javascript':
            continue
        if u.scheme and u.netloc: # Absolute href
            str_links.append(href)
        if not u.scheme and not u.netloc and u.path: # Relative href
            str_links.append(href)
    return str_links

def links_from_rss(markup):
    """
    Given a string representation of a document (rss) returns a list of all the links, relative and absolute. JavaScript links omitted.

    :param str markup: A valid rss document.

    :returns: A list of links as strings.
    :rtype: list( str )
    """
    entries = feedparser.parse(markup).entries
    return [e.link for e in entries if e.link]

def hrefs_from_links(base, links):
    """
    Given a list of links (relative and absolute) create an hrefs attribute.

    :param str base: The base for non-absolute urls. For example, http://www.buzzfeed.com/
    :param list( str ) links: The list of links in the document

    :rtype: list( tuple( str, int ) )
    :returns: A list of tuples of links and the frequency with which they appear
    """
    hrefs = defaultdict(int)
    if base.endswith('/'):
        base = base[0:-1]
    for link in links:
        if not urlparse(link).scheme:
            link = u"{0}{1}".format(base, link)
        try:
            canonical = canonicalize(link)
        except UnicodeDecodeError, e:
            logger.error(u"Failed to decode {0}".format(link))
            continue
        hrefs[canonical] += 1
    return hrefs.items()

def base_from_url(url):
    """
    Finds the base from a url to add to relative urls.

    :param str url: The absolute path from which to derive the base.

    :rtype: str
    :returns: The url base.
    """
    u = urlparse(url)
    return u"{0}://{1}".format(u.scheme, u.netloc)

def wordcounts_from_sgml(markup):
    """
    Given a body of markup; returns a dict of {unicode => int} representing the frequency of each word in the document.

    ILLEGAL_CHARS are replaced with '' and STOP_WORDS as well as non-alpha-numerics are not included.
    """
    text = BeautifulSoup(markup).get_text().lower()
    for char in ILLEGAL_CHARS:
        text.replace(char, '')

    all_words = re.split(r'\s+', text)
    counts = defaultdict(int)
    for word in all_words:
        if not word.isalnum():
            continue
        if word in STOP_WORDS:
            continue
        counts[word] += 1

    return counts

