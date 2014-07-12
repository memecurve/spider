from __future__ import absolute_import

from fabric.api import task
from subprocess import call

from fabfile.hbase import drop_tables, create_tables

@task
def integration(module=None):
    if module:
        call('nosetests --all-modules api/test/integration/{0}.py'.format(module), shell=True)
    else:
        call('nosetests --all-modules api/test/integration/', shell=True)


@task
def unit():
    call('nosetests --all-modules api/test/unit', shell=True)

@task
def all():
    integration()
    unit()
