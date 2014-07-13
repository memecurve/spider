import sys
from urlparse import urlparse
from collections import defaultdict

import feedparser
import requests
from bs4 import BeautifulSoup

from api import canonicalize

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
    return requests.get(url).content

def get_type(markup):
    """
    Returns the type of markup. Either SGML or RSS

    :param str markup: The markup for the document.

    :rtype: str
    :returns: One of 'rss' or 'sgml'
    """
    if not feedparser.parse(markup).version:
        return 'sgml'
    return 'rss'

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
            link = "{0}{1}".format(base, link)
        canonical = canonicalize(link)
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
    return "{0}://{1}".format(u.scheme, u.netloc)
