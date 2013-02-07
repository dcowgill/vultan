from fabric.api import env, local, put, run, settings, sudo
from fabric.contrib.project import rsync_project
from fabric.decorators import roles
import glob
import os
import time

env.roledefs = {
    'batch': ['merlin:6638'],
    'web': ['viper:6638', 'jester:6638']
    }

@roles('batch', 'web')
def deploy():
    'Deploys the source code to production.'
    with settings(user='twm'):
        rsync_project(
            remote_dir='/home/twm/work',
            local_dir=_basedir(),
            exclude=['.hg*', '*.pyc', '*.orig', '*ecss'],
            delete=True)

def _basedir():
    return os.path.dirname(os.path.abspath(__file__))
