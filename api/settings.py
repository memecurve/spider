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

LOG_LEVEL = logging.DEBUG
