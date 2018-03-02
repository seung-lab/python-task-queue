import json
from os.path import join

import requests

from .secrets import APPENGINE_QUEUE_URL, PROJECT_NAME, QUEUE_NAME

class AppEngineTaskQueueAPI(object):
    def __init__(self, project=PROJECT_NAME, queue_name=QUEUE_NAME):
        self._project = project 
        self._queue_name = queue_name

    def base_url(self):
        return join(APPENGINE_QUEUE_URL, '{PROJECT_ID}/taskqueue/{QUEUE_ID}/').format(
            PROJECT_ID=self._project,
            QUEUE_ID=self._queue_name,
        )

    @property
    def enqueued(self):
        tqinfo = self.status()
        return int(tqinfo['stats']['totalTasks'])

    def status(self):
        return requests.get(self.base_url()).json()

    def insert(self, body):
        if 'groupByTag' not in body or not body['groupByTag']:
            body['tag'] = ''
        
        if 'groupByTag' in body: del body['groupByTag']
        if 'queueName' in body: del body['queueName']

        url = join(self.base_url(), 'tasks')
        return requests.post(url, data=json.dumps(body)).json()

    def get(self, tid):
        endpoint = 'tasks/id/{TASK_ID}'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        return requests.get(url).json()

    def acknowledge(self, tid):
        return requests.delete(tid).json()

    def delete(self, tid):
        endpoint = 'tasks/id/{TASK_ID}'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        requests.delete(url)

    def purge(self):
        url = join(self.base_url(), 'purge')
        requests.post(url)

    def renew_lease(self, task_id, seconds):
        raise NotImplementedError()
        # endpoint = 'tasks/{TASK_ID}:renewLease'.format(TASK_ID=tid)
        # url = join(self.base_url(), endpoint)
        # return post(url, data=json.dumps({ 'leaseSecs': seconds }))

    def cancel_lease(self, task_id):
        raise NotImplementedError()
        # endpoint = 'tasks/{TASK_ID}:cancelLease'.format(TASK_ID=tid)
        # url = join(self.base_url(), endpoint)
        # return post(url)

    def lease(self, numTasks=1, leaseSecs=1, groupByTag=False, tag=''):
        if groupByTag == False:
            tag = ''
        
        params = {
            'numTasks': numTasks, 
            'leaseSecs': leaseSecs, 
            'tag': tag,
        }

        url = join(self.base_url(), 'tasks/lease')
        return requests.post(url, data=json.dumps(params)).json()

    def list(self):
        url = join(self.base_url(), 'tasks')
        return requests.get(url).json()


class ExecuteWrapper(object):
    def __init__(self, result):
        self.result = result
    def execute(self, num_retries=6):
        if type(self.result) is list and len(self.result):
            # mimicking  Google's Pull Queue API
            return { 'items': self.result } 
        return self.result
