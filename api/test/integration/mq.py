import unittest
import time
import os
import sys

from api.services import mq
from api.services.db import document

from api.models import Document

from api.exceptions import StopConsuming
"""
global queued
queued = []

class TestMQ(unittest.TestCase):

    def test_queue_and_consume(self):
        mq.Consumer.TIMEOUT = 2
        p = mq.Producer()
        urls = ['http://www.example.com/', 'http://www.google.com/', 'http://www.yahoo.com/', 'http://www.netflix.com/', 'http://www.hulu.com/']

        def write_document(msg):
            url = msg.get('url', None)
            c = Document(url=url,
                         markup='<xml></xml>',
                         hrefs=[],
                         type='rss', updated_at=123456789)
            _ = document.create(c, created_at=123456789)
            return True

        for url in urls:
            p.send({'url': url})

        for url in urls:
            mq.pass_urls(write_document, daemonize=False)

        docs = document.find(type='rss', updated_at__gte=123456788, updated_at__lte=123456790)

        self.assertEquals(len(docs), 5)

        for d in docs:
            self.assertIn(d.url, urls)
"""
