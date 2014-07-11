from fabric.api import task

from subprocess import call

@task
def integration():
    call('nosetests --all-modules api/test/integration', shell=True)
