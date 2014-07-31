"""
from __future__ import absolute_import

import sys
import unittest

from api.services.db import HbaseInternals

class TestHbaseInternals(unittest.TestCase):

    def setUp(self):
        self.i = HbaseInternals()
        self.test_table = 'test'
        try:
            self.i.delete_table(self.test_table)
        except Exception, e:
            pass
        try:
            self.i.create_table(self.test_table, {'cf': {}})
        except Exception, e:
            pass
        self.expected = [
            ('row-key-1', {'cf:col1': 'value1', 'cf:col2': 'value2'}),
            ('row-key-2', {'cf:col1': 'value3', 'cf:col2': 'value4'}),
            ('row-key-3', {'cf:col1': 'value5', 'cf:col2': 'value6'}),
            ('row-key-4', {'cf:col1': 'value7', 'cf:col2': 'value8'}),
            ('row-key-5', {'cf:col1': 'value9', 'cf:col2': 'value10'}),
            ]

        for row_key, doc in self.expected:
            self.i.write(self.test_table, row_key=row_key, doc=doc)

    def tearDown(self):
        rows = self.i.find(self.test_table, row_prefix='row')
        for row_key, doc in rows:
            self.i.delete_one(self.test_table, row_key)

    def map_observed(self, observed):
        try:
            return {k: v for k, v in observed.items()}
        except Exception, e:
            print "Observed:", observed
            raise e

    def test_read_and_write_one(self):
        for row_key, expected_doc in self.expected:
            found = self.i.find_one(self.test_table, row_key)
            observed_doc = self.map_observed(found)
            self.assertEquals(observed_doc, expected_doc, "Got {0} and expected {1}".format(observed_doc, expected_doc))

    def test_find_by_col_prefix(self):
        rows = self.i.find(self.test_table, row_prefix='row')
        mapped_rows = []
        for row_key, doc in rows:
            mapped_rows.append(self.map_observed(doc))

        expected = sorted(map(str, [e[1] for e in self.expected]))
        observed = sorted(map(str, mapped_rows))

        self.assertEquals(expected, observed)

    def test_delete_one(self):
        self.i.delete_one(self.test_table, 'row-key-3')
        for row_key, expected_doc in self.expected:
            observed_doc = self.i.find_one(self.test_table, row_key)
            if row_key == 'row-key-3':
                print "Found doc with row key: row-key-3:", observed_doc
                self.assertEquals(observed_doc, {})
            else:
                self.assertEquals(self.map_observed(observed_doc), expected_doc)

    def test_write_many(self):
        addtl_expected = [
            ('row-key-6', {'cf:col1': 'value6', 'cf:col2': 'value11'}),
            ('row-key-7', {'cf:col1': 'value7', 'cf:col2': 'value12'}),
            ('row-key-8', {'cf:col1': 'value8', 'cf:col2': 'value13'}),
            ('row-key-9', {'cf:col1': 'value9', 'cf:col2': 'value14'}),
            ('row-key-10', {'cf:col1': 'value10', 'cf:col2': 'value15'}),
            ]

        success = self.i.write_many(self.test_table, addtl_expected)
        self.assertEquals(success, True)

        found = self.i.find_one(self.test_table, 'row-key-6')
        self.assertEquals(self.map_observed(found), addtl_expected[0][1])

        found = self.i.find_one(self.test_table, 'row-key-7')
        self.assertEquals(self.map_observed(found), addtl_expected[1][1])

        found = self.i.find_one(self.test_table, 'row-key-8')
        self.assertEquals(self.map_observed(found), addtl_expected[2][1])

        found = self.i.find_one(self.test_table, 'row-key-9')
        self.assertEquals(self.map_observed(found), addtl_expected[3][1])

        found = self.i.find_one(self.test_table, 'row-key-10')
        self.assertEquals(self.map_observed(found), addtl_expected[4][1])

    def test_inc_and_dec(self):
        val = self.i.inc(self.test_table, 'row-key-11', 'cf', how_much=10)
        self.assertEquals(val, 10)

        val = self.i.inc(self.test_table, 'row-key-11', 'cf', how_much=5)
        self.assertEquals(val, 15)

        val = self.i.inc(self.test_table, 'row-key-11', 'cf', how_much=1)
        self.assertEquals(val, 16)

        val = self.i.dec(self.test_table, 'row-key-11', 'cf', how_much=16)
        self.assertEquals(val, 0)

        val = self.i.dec(self.test_table, 'row-key-11', 'cf', how_much=1)
        self.assertEquals(val, -1)

if __name__ == '__main__':
    unittest.main()
"""
