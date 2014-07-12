import sys
import feedparser
import requests

from urlparse import urlparse
from bs4 import BeautifulSoup

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

