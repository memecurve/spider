import unittest
import sys

from api.services.db import Internals
from api.models import Document
from api.services.db import document

class DocumentTest(unittest.TestCase):

    def test_create_and_find(self):
        try:
            updated_at = 12345678
            a = Document(url='http://www.buzzfeed.com', markup='<html></html>',
                         hrefs=[('http://www.google.com/', 1), ('http://www.facebook.com/', 10), ('http://www.yahoo.com/', 5)],
                         type='sgml', updated_at=updated_at)
            b = Document(url='http://www.hulu.com', markup='<html><head></head><body></body></html>',
                         hrefs=[('http://www.facebook.com/', 100), ('http://www.gmail.com/', 99), ('http://www.youtube.com/', 1)],
                         type='sgml', updated_at=updated_at)
            c = Document(url='http://www.feedburner.com/', markup='<xml></xml>',
                         hrefs=[('http://nytimes.com', 10), ('http://usatoday.com', 50)],
                         type='rss', updated_at=updated_at)

            a_, errors = document.create(a, updated_at)
            self.assertEquals(errors, [])
            b_, errors = document.create(b, updated_at)
            self.assertEquals(errors, [])
            c_, errors = document.create(c, updated_at)
            self.assertEquals(errors, [])

            docs, errors = document.find(type='sgml',
                                         updated_at__lte=12345679,
                                         updated_at__gte=12345677,
                                         include_links=True,
                                         include_markup=True)

            self.assertEquals(errors, [])
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

            docs, errors = document.find(type=c_.type,
                                         updated_at__lte=12345679,
                                         updated_at__gte=12345677,
                                         include_links=False,
                                         include_markup=False)

            self.assertEquals(errors, [])
            self.assertEquals(docs[0].to_dict(), {"url": "http://www.feedburner.com/", "hrefs": [], "type": "rss", "updated_at": 12345678})
        finally:
            internals = Internals()
            for d in [a_, b_, c_]:
                rk = "{0}{1}{2}".format(d.type, d.updated_at, d.url)
                internals.delete_one(document.TABLE_NAME, rk)