import unittest
import sys

from api.services.db import HbaseInternals
from api.models import Document
from api.services.db import document
from api import canonicalize


class DocumentTest(unittest.TestCase):

    def setUp(self):
        updated_at = 12345678
        a = Document(url=canonicalize('http://www.buzzfeed.com/'), markup='<html></html>',
                     hrefs=[('http://www.google.com/', 1), ('http://www.facebook.com/', 10), ('http://www.yahoo.com/', 5)],
                     type='sgml', updated_at=updated_at)
        b = Document(url=canonicalize('http://www.hulu.com/'), markup='<html><head></head><body></body></html>',
                     hrefs=[('http://www.facebook.com/', 100), ('http://www.gmail.com/', 99), ('http://www.youtube.com/', 1)],
                     type='sgml', updated_at=updated_at)
        c = Document(url=canonicalize('http://www.feedburner.com/'), markup='<xml></xml>',
                     hrefs=[('http://nytimes.com', 10), ('http://usatoday.com', 50)],
                     type='rss', updated_at=updated_at)

        a_ = document.create(a, updated_at)
        b_ = document.create(b, updated_at)
        c_ = document.create(c, updated_at)

        self.a = a
        self.b = b
        self.c = c
        self.a_ = a_
        self.b_ = b_
        self.c_ = c_
        self.updated_at = updated_at

    def tearDown(self):
        internals = HbaseInternals()
        for d in [self.a_, self.b_, self.c_]:
            rk = "{0}{1}{2}".format(d.type, d.updated_at, canonicalize(d.url))

            internals.delete_one(document.TABLE_NAME, rk)

    def test_row_range(self):
        start_row, end_row = document._get_row_range(type='rss', updated_at__lte=12345, updated_at__gte=10000, url='http://www.google.com/')
        self.assertEquals(start_row, 'rss10000http://www.google.com/')
        self.assertEquals(end_row, 'rss12345http://www.google.com/')

    def test_find_by_url(self):
        i = HbaseInternals()

        a__ = document.find_by_url(url=canonicalize('http://www.buzzfeed.com'),
            updated_at__gte=self.updated_at - 1, updated_at__lte=self.updated_at + 1)
        b__ = document.find_by_url(url=canonicalize('http://www.hulu.com'),
            updated_at__gte=self.updated_at - 1, updated_at__lte=self.updated_at + 1)
        c__ = document.find_by_url(url=canonicalize('http://www.feedburner.com'),
            updated_at__gte=self.updated_at - 1, updated_at__lte=self.updated_at + 1)


        self.assertEquals(self.a_.url, a__.url)
        self.assertEquals(self.b_.url, b__.url)
        self.assertEquals(self.c_.url, c__.url)


    def test_create_and_find(self):
        docs = [d for d in document.find(type='sgml',
                                     updated_at__lte=self.updated_at + 1,
                                     updated_at__gte=self.updated_at - 1,
                                     include_links=True,
                                     include_markup=True)]

        self.assertEquals(len(docs), 2)

        for doc in docs:
            if 'buzzfeed' in doc.url:
                self.assertEquals(doc.markup, '<html></html>')
                self.assertEquals(doc.type, 'sgml')
                self.assertEquals(dict(doc.hrefs), dict([('http://www.google.com/', 1), ('http://www.facebook.com/', 10), ('http://www.yahoo.com/', 5)]))
                self.assertTrue(isinstance(doc.updated_at, int))
            else:
                self.assertIn('hulu', doc.url)
                self.assertEquals(dict(doc.hrefs), dict([('http://www.facebook.com/', 100), ('http://www.gmail.com/', 99), ('http://www.youtube.com/', 1)]))
                self.assertEquals(doc.type, 'sgml')
                self.assertTrue(isinstance(doc.updated_at, int))

        docs = [d for d in document.find(type=self.c_.type,
                                     updated_at__lte=self.updated_at + 1,
                                     updated_at__gte=self.updated_at - 1,
                                     include_links=False,
                                     include_markup=False)]

        self.assertEquals(docs[0].to_dict(), {"url": "http://www.feedburner.com/", "hrefs": [], "type": "rss", "updated_at": self.updated_at})


