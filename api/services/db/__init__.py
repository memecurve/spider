import datetime
import sys
import time
import pytz

import happybase

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase

from api.settings import (
    HBASE_HOST,
    HBASE_PORT,
    HBASE_TABLE_PREFIX,
    HBASE_CONN_POOL_SIZE,
    HBASE_BATCH_SIZE,
    THRIFT_HOST,
    THRIFT_PORT,
)

LOCAL_TZ = pytz.utc

def _safe_happybase_call(f):
    def _f(*args, **kwargs):
        try:
            return f(*args, **kwargs), []
        except (RuntimeError, ValueError, happybase.NoConnectionsAvailable), e: # Happybase uses generic errors :(
            return None, [str(e)]
    return _f

class HbaseInternals(object):

    _instance = None

    def __init__(self):
        self.__transport = TBufferedTransport(TSocket(THRIFT_HOST, THRIFT_PORT))
        self.__transport.open()
        self.__protocol = TBinaryProtocol.TBinaryProtocol(self.__transport)
        self.__client = Hbase.Client(self.__protocol)
        self.__table_prefix = HBASE_TABLE_PREFIX

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(HbaseInternals, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def write(self, table, row_key, doc):
        """
        Write a document to a row key (put).

        :param unicode table: The table to write to
        :param unicode row_key: The key for the row we're modifying
        :param dict(unicode, unicode) doc: A dictionary of columnfamily:columndescriptor, columnvalue pairs of type unicode.

        :returns: Nothing
        :rtype: None
        """
        mutations = []
        for column_key, value in doc.iteritems():
            mutations.append(Hbase.Mutation(column=column_key, value=value))
        self.__client.mutateRow("_".join([self.__table_prefix, table]), row_key, mutations)

    def find_one(self, table, row_key):
        """
        Returns a single row by row_key

        :param unicode table: The table to find a row from
        :param unicode row_key: The key for the row.

        :rtype: dict( unicode, unicode )
        :returns: The document at the row
        """
        scan = None
        try:
            scan = self.__client.scannerOpen('_'.join([self.__table_prefix, table]), row_key, [])
            row_list = self.__client.scannerGetList(scan, 1)
            for row in row_list:
                if row.row == row_key:
                    return {col: tcell.value for col, tcell in row.columns.iteritems()}
                else:
                    return {}
        except Hbase.TApplicationException, e:
            pass
        finally:
            if scan is not None: self.__client.scannerClose(scan)

    def find(self, table, row_prefix=None, row_start=None, row_stop=None, columns=None, limit=None, column_filter=None):
        """
        Returns a cursor of value, timestamp from a table by row prefix.
        """
        if columns is None:
            columns = []
        if column_filter is not None:
            column, column_value = column_filter
        sys.stderr.write("{0}\n".format(column_filter))
        scan = None
        try:
            sys.stderr.write("Row prefix: {0}\n".format(row_prefix or row_start))
            scan = self.__client.scannerOpenWithPrefix('_'.join([self.__table_prefix, table]),
                                                       startAndPrefix=row_prefix or row_start,
                                                       columns=columns)
            while limit is None or limit >= 0:
                if limit is not None: limit -= 1
                row_list = self.__client.scannerGetList(scan, 1)
                if not row_list:
                    sys.stderr.write("No row list.\n")
                    break
                for row in row_list:
                    sys.stderr.write("Got row: {0}\n".format(row))
                    if row_stop is not None and row.row.startswith(row_stop):
                        sys.stderr.write("Stopping iteration\n")
                        raise StopIteration('done')
                    result = (row.row, {col: tcell.value for col, tcell in row.columns.iteritems()})
                    if column_filter is not None and result:
                        if result[1][column] == column_value:
                            yield result
                        else:
                            sys.stderr.write("Skipping: {0}={1}: {2}".format(column, column_value, result))
                            continue
                    else:
                        yield result

        except Hbase.TApplicationException, e:
            sys.stderr.write("Application exception\n")
            pass
        finally:
            if scan is not None: self.__client.scannerClose(scan)

    def delete_one(self, table, row_key):
        return self.__client.deleteAllRow("_".join([self.__table_prefix, table]), row_key)

    def inc(self, table, row_key, column_family, how_much=0):
        return self.__client.atomicIncrement('_'.join([self.__table_prefix, table]), row_key, column_family, how_much)

    def dec(self, table, row_key, column_family, how_much=0):
        return self.__client.atomicIncrement('_'.join([self.__table_prefix, table]), row_key, column_family, -how_much)

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
        :param int timestamp: The timestamp to assign as the timestamp value for the batch. Defaults to the current time. Not working at the moment.

        :returns: A tuple of True, [] if every spec succeeds. Otherwise False, [str] (with an error message)
        """
        try:
            all_mutations = []
            for row_key, doc in specs:
                mutations = []
                for column_key, value in doc.iteritems():
                    mutations.append(Hbase.Mutation(column=column_key, value=value))
                all_mutations.append(Hbase.BatchMutation(row=row_key, mutations=mutations))

            self.__client.mutateRows('_'.join([self.__table_prefix, table]), all_mutations)
            return True
        except ValueError, e:
            return False


class HappybaseInternals(object):

    _instance = None

    def __init__(self):
        self.__conn_pool = None

    def __new__(cls, *args, **kwargs):
        """
        Makes HappybaseInternals a singleton
        """
        if not cls._instance:
            cls._instance = super(HappybaseInternals, cls).__new__(
                                cls, *args, **kwargs)
        return cls._instance

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

    @_safe_happybase_call
    def write(self, table, row_key, doc):
        """
        Write a document to a row key 
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).put(row_key, doc)

    @_safe_happybase_call
    def find_one(self, table, row_key):
        """
        Returns a single tuple of value, timestamp by a row key.
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).row(row_key, include_timestamp=True)

    @_safe_happybase_call
    def find(self, table, row_prefix=None, row_start=None, row_stop=None, columns=None, limit=None, filter=None):
        """
        Returns a cursor of value, timestamp from a table by row prefix.
        """
        params = {k: v for k, v in locals().items() if v is not None}
        params['include_timestamp'] = True
        del params['table']
        del params['self']
        with self.conn_pool().connection() as conn:
            return conn.table(table).scan(**params)

    @_safe_happybase_call
    def delete_one(self, table, row_key):
        """
        Delete a record from a table by row key. This creates a tombstone to be actually deleted upon rebalancing.
        """
        with self.conn_pool().connection() as conn:
            return conn.table(table).delete(row_key)

    @_safe_happybase_call
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
        :param int timestamp: The timestamp to assign as the timestamp value for the batch. Defaults to the current time. Not working at the moment.
        
        :returns: A tuple of True, [] if every spec succeeds. Otherwise False, [str] (with an error message)
        """
        if timestamp is None:
            timestamp = int(time.mktime(datetime.datetime.utcnow().timetuple()))
        try:
            with self.conn_pool().connection() as conn:
                with conn.table(table).batch(batch_size=HBASE_BATCH_SIZE) as b: # TODO: Figure out why timestamp doesn't work
                    for row_key, doc in specs:
                        b.put(row_key, doc)
        except ValueError, e:
            return False
        return True

    @_safe_happybase_call
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

    @_safe_happybase_call
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


    def get_timestamp(self):
        return int(time.mktime(datetime.datetime.now().replace(tzinfo=LOCAL_TZ).timetuple()))

    def delete_table(self, table):
        """
        Disables and deletes an entire table.

        :param str table: The name of the table to delete.

        """
        with self.conn_pool().connection() as conn:
            return conn.delete_table(table, disable=True)

    def create_table(self, table, descriptors):
        """
        Creates a table.

        """
        with self.conn_pool().connection() as conn:
            return conn.create_table(table, descriptors)


