from subprocess import call

from fabric.api import task
import happybase

from api.settings import ( 
    HBASE_HOST,
    HBASE_PORT,
    HBASE_TABLE_PREFIX,
    HBASE_SCHEMA
)


@task
def create_tables():
    conn = happybase.Connection(table_prefix=HBASE_TABLE_PREFIX, 
        host=HBASE_HOST, 
        port=HBASE_PORT)

    for table_name, column_families in HBASE_SCHEMA: 
        print "Creating table {0}".format(table_name)
        try:
            conn.create_table(table_name, column_families)
        except happybase.hbase.ttypes.AlreadyExists:
            pass
        print "Done."

@task
def drop_tables():
    conn = happybase.Connection(table_prefix=HBASE_TABLE_PREFIX, 
        host=HBASE_HOST, 
        port=HBASE_PORT)
    for table_name, column_families in HBASE_SCHEMA:
        print "Dropping table {0}".format(table_name)
        try:
            conn.delete_table(table_name, disable=True)
        except happybase.hbase.ttypes.IOError, e:
            pass
        print "Done."

@task
def start():
    call("start-hbase.sh", shell=True)
    call("hbase thrift start -threadpool", shell=True)
