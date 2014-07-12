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

def extract_links(url, markup):
    """
    Given a string representation of a document (rss or sgml) returns a list of all the links on the page, with relative links re-written to include the domain.

    :param str url: The url that points to the markup
    :param str markup: The document at the url.

    :returns: A list of links as strings.
    :rtype: list( str )
    """
    # TODO: Finish this method
    parent_link = urlparse(url)
    base = "{0}://{1}".format(parent_link.scheme, parent_link.netloc)
    bs4_links = BeautifulSoup(markup).find_all('a')
    str_links = []
    for link in bs4_links:
        href = link['href']
        u = urlparse(href)
        if u.scheme.lower() == 'javascript':
            continue
        if u.scheme and u.netloc:
            str_link = href
        if not u.scheme or not u.netloc:
            str_link = "{0}{1}?{2}".format(base, u.path, u.query)
        str_links.append(str_link)
    return str_links



