import unittest
import json

from api.models import Model

class Document(Model):
    REQUIRED_FIELDS = {'url', 'markup'}
    OPTIONAL_FIELDS = {'hrefs'}

from api.exceptions import ExtraParameters
from api.exceptions import MissingParameters

class TestModel(unittest.TestCase):

    def test_success_without_optional(self):
        d = Document(url='http://www.example.com/', markup='<html></html>')
        self.assertEquals(d.url, 'http://www.example.com/')
        self.assertEquals(d.markup, '<html></html>')
        self.assertFalse(hasattr(d, 'hrefs'))

    def test_success_with_optional(self):
        d = Document(url='http://www.example.com/', markup='<html></html>',
                     hrefs=[{'google.com': 1}, {'yahoo.com': 10}])
        self.assertEquals(d.url, 'http://www.example.com/')
        self.assertEquals(d.markup, '<html></html>')
        self.assertEquals(d.hrefs[0]['google.com'], 1)
        self.assertEquals(d.hrefs[1]['yahoo.com'], 10)

    def test_extra_fields(self):
        with self.assertRaises(ExtraParameters):
            Document(url='http://www.example.com', markup='<html></html>', foo='bad parameter')
        with self.assertRaises(ExtraParameters):
            Document(url='http://www.example.com', markup='<html></html>',
                     hrefs=[{'google.com': 1}, {'yahoo.com': 10}], foo='bad parameter')

    def test_missing_fields(self):
        with self.assertRaises(MissingParameters):
            Document(url='http://www.example.com')
        with self.assertRaises(MissingParameters):
            Document(url='http://www.example.com',
                     hrefs=[{'google.com': 1}, {'yahoo.com': 10}])

    def test_to_dict(self):
        d = Document(url='http://www.example.com/', markup='<html></html>',
                     hrefs=[{'google.com': 1}, {'yahoo.com': 10}])
        self.assertEquals(d.to_dict(), {'url': 'http://www.example.com/', 'markup': '<html></html>',
                                        'hrefs': [{'google.com': 1}, {'yahoo.com': 10}]})

    def test_to_json(self):
        d = Document(url='http://www.example.com/', markup='<html></html>',
                     hrefs=[{'google.com': 1}, {'yahoo.com': 10}])
        self.assertEquals(d.to_json(), json.dumps({'url': 'http://www.example.com/', 'markup': '<html></html>',
                                                   'hrefs': [{'google.com': 1}, {'yahoo.com': 10}]}))


