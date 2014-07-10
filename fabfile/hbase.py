from api.settings import ( 
    HBASE_HOST,
    HBASE_PORT,
    HBASE_TABLE_PREFIX,
    HBASE_SCHEMA
)

from fabric.api import task

import happybase

@task
def create_table():
    conn = happybase.Connection(table_prefix=HBASE_TABLE_PREFIX, 
        host=HBASE_HOST, 
        port=HBASE_PORT)

    for table_name, column_families in HBASE_SCHEMA: 
        print "Creating table {0}".format(table_name)
        conn.create_table(table_name, column_families)
        print "Done." 

