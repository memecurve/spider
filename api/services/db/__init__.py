import datetime
import sys
import time

import happybase

from api.settings import (
    HBASE_HOST,
    HBASE_PORT,
    HBASE_TABLE_PREFIX,
    HBASE_CONN_POOL_SIZE,
    HBASE_BATCH_SIZE
)

class Internals(object):

    def __init__(self):
        self.__conn_pool = None

    def conn_pool(self):
        """
        Connect to the database. If there's already a connection just return it.
        """
        if not self.__conn_pool:
            self.__conn_pool = happybase.ConnectionPool(size=HBASE_CONN_POOL_SIZE, 
                host=HBASE_HOST, 
                port=HBASE_PORT, 
                table_prefix=HBASE_TABLE_PREFIX, 
                table_prefix_separator='_')
        return self.__conn_pool

    def write(self, table, row_key, doc):
        """
        Write a document to a row key 
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).put(row_key, doc)

    def find_one(self, table, row_key):
        """
        Returns a single tuple of value, timestamp by a row key.
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).row(row_key, include_timestamp=True)

    def find(self, table, rowprefix):
        """
        Returns a cursor of value, timestamp from a table by row prefix.
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).scan(rowprefix, include_timestamp=True)

    def delete_one(self, table, row_key):
        """
        Delete a record from a table by row key. This creates a tombstone to be actually deleted upon rebalancing.
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).delete(row_key)

    def write_many(self, table, specs, timestamp=None):
        """
        Implements a batched write to the database. 
        
        Updating and deleting a row in a single batch results in undefined behavior. 

        The default batch size is set in the api settings file.

        :param str table: The table to write to
        :param specs: A list of tuples of rowid, doc pairs.

        .. code-block:: javascript
            [
                ('row-key-1', {'cf:col1': 'value1', 'cf:col2': 'value2'}),
                ('row-key-2', {'cf:col2': 'value2', 'cf:col3': 'value3'}),
                ('row-key-3', {'cf:col3': 'value3', 'cf:col4': 'value4'})
            ]

        :type specs: list( tuple( str, dict ) )
        :param int timestamp: The timestamp to assign as the timestamp value for the batch. Defaults to the current time.
        
        :returns: A tuple of True, [] if every spec succeeds. Otherwise False, [str] (with an error message)
        """
        if timestamp is None:
            timestamp = int(time.mktime(datetime.datetime.utcnow().timetuple()))
        with self.conn_pool().connection() as conn:
            with conn.table(table).batch(timestamp=timestamp, batch_size=HBASE_BATCH_SIZE) as b:
                for row_key, doc in specs:
                    sys.stderr.write("<{0}: {1}>\n".format(row_key, doc))
                    b.put(row_key, doc)
        return True, []

    def inc(self, table, row_key, column_family, how_much=0):
        """
        Increments a counter atomically. The counter is an 8-byte wide value. HBase treats them as big endian 64b signed ints. 

        Initialized to 0 when first used. 

        :param str table: The table the counter is stored in
        :param str row_key: The row key for the counter in that table
        :param str column_family: The column family for the counter
        :param int how_much: How much to increment the counter by.

        :returns: The value of the counter after updating and an empty list, or False and a list of errors.
        :rtype: tuple(int, list)
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).counter_inc(row_key, column_family, how_much)

    def dec(self, table, row_key, column_family, how_much=0):
        """
        Decrements a counter atomically. The counter is an 8-byte wide value. HBase treats them as big endian 64b signed ints. 

        Initialized to 0 when first used. 

        :param str table: The table the counter is stored in
        :param str row_key: The row key for the counter in that table
        :param str column_family: The column family for the counter
        :param int how_much: How much to decrement the counter by.

        :returns: The value of the counter after updating and an empty list, or False and a list of errors.
        :rtype: tuple(int, list)
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).counter_dec(row_key, column_family, value=how_much)

