import unittest
import sys

from api.services import http

from api.test import fixtures

class HttpTest(unittest.TestCase):

    def test_get_type(self):
        self.assertEquals(http.get_type(fixtures.atom03), 'rss')
        self.assertEquals(http.get_type(fixtures.atom10), 'rss')
        self.assertEquals(http.get_type(fixtures.html5), 'sgml')
        self.assertEquals(http.get_type(fixtures.rss20), 'rss')
        self.assertEquals(http.get_type(''), 'sgml')

    def test_links_from_sgml(self):
        links = http.links_from_sgml(fixtures.html5)
        absolute = 'http://www.buzzfeed.com/username/post-slug'
        relative = '/username/post-slug'
        path_only = '/'

        self.assertIn(absolute, links)
        self.assertIn(relative, links)
        self.assertIn(path_only, links)
        self.assertNotIn('javascript:;', links)
        self.assertNotIn('javascript: void(0);', links)

    def test_links_from_rss(self):
        rss20_links = http.links_from_rss(fixtures.rss20)
        expected = ["http://www.example.com/blog/post/1"]
        self.assertEquals(rss20_links, expected)

        atom10_links = http.links_from_rss(fixtures.atom10)
        expected = ["http://example.org/2003/12/13/atom03.html"]
        self.assertEquals(atom10_links, expected)

        atom03_links = http.links_from_rss(fixtures.atom03)
        expected = ["http://www.example.com/item1-info-page.html"]
        self.assertEquals(atom03_links, expected)

    def test_hrefs_from_links(self):
        base = 'http://buzzfeed.com/'
        links = ['/username/post-slug', 'http://www.google.com/',
                 'http://www.gmail.com?a=1&b=2',
                 'http://www.buzzfeed.com/username/post-slug?b=2&a=2&utm_term=foobar#bizbaz']

        self.assertEquals(http.hrefs_from_links(base, links),
                          [('http://buzzfeed.com/username/post-slug', 1),
                           ('http://www.gmail.com/?a=1&b=2', 1),
                           ('http://www.google.com/', 1),
                           ('http://www.buzzfeed.com/username/post-slug?a=2&b=2', 1)])
