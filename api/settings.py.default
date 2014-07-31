import logging

HBASE_HOST = '127.0.0.1'
HBASE_PORT = 9090
HBASE_TABLE_PREFIX = 'spider'
HBASE_CONN_POOL_SIZE = 3
HBASE_BATCH_SIZE = 100

HBASE_SCHEMA = [
    ('test', {'cf': {}}),
    ('document', {'self': {'max_versions': 1}, # self:url, self:type, self:updated_at
                  'data': {'max_versions': 1},
                  'href': {'max_versions': 1} })# [{href:www.buzzfeed.com: 1}] List of dicts})
]

RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_URL_QUEUE = 'download-tasks'

THRIFT_HOST = 'localhost'
THRIFT_PORT = 9090

LOG_LEVEL = logging.DEBUG


"""
Spider settings.

"""
DISCOVER_NEW = False # Should we queue new links as they're discovered?
MAX_DOWNLOADS = int(1e6) # Maximum amount to queue when DISCOVER_NEW is False
CYCLE_RESOLUTION = 24*60*60#60 * 60 * 24 # Frequency with which the workers are restarted. Determines a sensible number of documents to attempt to download based on the amount we managed last time.
