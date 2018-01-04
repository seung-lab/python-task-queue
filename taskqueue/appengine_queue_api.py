import json

# Python2/3 compat
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

import requests

from httplib2 import Http
from googleapiclient.http import HttpRequest

from .secrets import APPENGINE_QUEUE_URL


class ExecuteWrapper(object):
    def __init__(self, result):
        self.result = result
    def execute(self, num_retries=6):
        if type(self.result) is list and len(self.result):
            # mimicking  Google's Pull Queue API
            return { 'items': self.result } 
        return self.result

class AppEngineTaskQueue():

    class Tasks():
        def __init__(self, appengine):
            self._ae = appengine

        def insert(self,  project, taskqueue, body):

            if 'groupByTag' not in body or not body['groupByTag']:
                body['tag'] = ''
            
            if 'groupByTag' in body: del body['groupByTag']
            if 'queueName' in body: del body['queueName']

            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks'.format(project, taskqueue)
            r = requests.post(url, data=json.dumps(body))
            return ExecuteWrapper(r.json())

        def get(self, project, taskqueue, task):
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks/{}'.format(
                project, taskqueue, task
            )
            r = requests.get(url, params=json.dumps(body))
            return ExecuteWrapper(r.json())

        def delete(self, project, taskqueue, task):
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks/{}'.format(project, taskqueue, task)
            r = requests.delete(url)
            return ExecuteWrapper(None)

        def lease(self, project, taskqueue, numTasks=1, leaseSecs=1, groupByTag=False, tag=''):

            if groupByTag == False:
                tag = ''
            
            params = {
                'numTasks': numTasks, 
                'leaseSecs': leaseSecs, 
                'tag': tag,
            }
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks/lease'.format(
                project, taskqueue
            )
            r = requests.post(url, params=params)
            return ExecuteWrapper(r.json())

        def list(self, project, taskqueue):
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks'.format(
                project, taskqueue
            )
            r = requests.get(url)
            return ExecuteWrapper(r.json())

    class TaskQueues():
        def __init__(self, appengine):
            self._ae = appengine

        def get(self, project, taskqueue, getStats=False):
            url = self._ae._queue_url + '/{}/taskqueue/{}'.format(
                project, taskqueue
            )
            r = requests.get(url)
            return ExecuteWrapper(r.json())

    def __init__(self):
        self._queue_url = APPENGINE_QUEUE_URL
        self._tasks = AppEngineTaskQueue.Tasks(self)
        self._taskqueues = AppEngineTaskQueue.TaskQueues(self)

    def tasks(self):
        return self._tasks

    def taskqueues(self):
        return self._taskqueues
