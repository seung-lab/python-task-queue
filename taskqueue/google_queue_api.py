from __future__ import print_function

from os.path import join

import json

# Python2/3 compat
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

from google.auth.transport.requests import AuthorizedSession
from cloudvolume import ConnectionPool

from .secrets import google_credentials, PROJECT_NAME, QUEUE_NAME

class GoogleSessionConnectionPool(ConnectionPool):
    def _create_connection(self):
        # scopes needed
        # 'https://www.googleapis.com/auth/cloud-platform',
        #  'https://www.googleapis.com/auth/cloud-tasks',
        project, credentials = google_credentials()
        return AuthorizedSession(credentials)
    
    def _close_function(self):
        return lambda conn: conn.close()

POOL = GoogleSessionConnectionPool()

def get(url, *args, **kwargs):
    return _netdo('get', url, *args, **kwargs)
def post(url, *args, **kwargs):
    return _netdo('post', url, *args, **kwargs)
def delete(url, *args, **kwargs):
    return _netdo('delete', url, *args, **kwargs)

def _netdo(fn, url, *args, **kwargs):
    global POOL
    session = POOL.get_connection()
    resp = getattr(session, fn)(url, *args, **kwargs)
    POOL.release_connection(session)
    return resp.json()

class GoogleTaskQueueAPI(object):
    def __init__(self, project=PROJECT_NAME, queue_name=QUEUE_NAME):
        self._project = project 
        self._location = 'us-central1' # only us-central1 is functional during the alpha
        self._queue_name = queue_name

    def base_url(self):
        return 'https://cloudtasks.googleapis.com/v2beta2/projects/{PROJECT_ID}/locations/{LOCATION_ID}/queues/{QUEUE_ID}'.format(
            PROJECT_ID=self._project,
            LOCATION_ID=self._location,
            QUEUE_ID=self._queue_name,
        )

    def status(self):
        """
        Using getStats

        The getStats feature of the Task Queue REST API (v1) is not yet supported in Cloud Tasks API (v2). 
        As a workaround, you can use the existing QueueStatistics class available in the App Engine SDK. 
        However, Google does not recommend using the results of either getStats or QueueStatistics as 
        the basis of programmatic decision-making in your code, because these methods return only 
        approximations and even these approximations are subject to delay.
        """
        pass

    def insert(self, body):
        if 'groupByTag' not in body or not body['groupByTag']:
            body['tag'] = ''
        
        if 'groupByTag' in body: del body['groupByTag']
        if 'queueName' in body: del body['queueName']

        url = join(self.base_url(), 'tasks')
        return post(url, data=json.dumps(body))

    def get(self, tid):
        endpoint = 'tasks/{TASK_ID}'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        return get(url)

    def acknowledge(self, tid):
        endpoint = 'tasks/{TASK_ID}:acknowledge'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        return post(url)

    def delete(self, tid):
        endpoint = 'tasks/{TASK_ID}'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        return delete(url)

    def renew_lease(self, task_id, seconds):
        endpoint = 'tasks/{TASK_ID}:renewLease'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        return post(url, data=json.dumps({ 'leaseSecs': seconds }))

    def cancel_lease(self, task_id):
        endpoint = 'tasks/{TASK_ID}:cancelLease'.format(TASK_ID=tid)
        url = join(self.base_url(), endpoint)
        return post(url)

    def lease(self, numTasks=1, seconds=1, groupByTag=False, tag=''):
        if groupByTag == False:
            tag = ''
        
        params = {
            'numTasks': numTasks, 
            'leaseSecs': leaseSecs, 
            'tag': tag,
        }

        url = join(self.base_url(), 'tasks:lease')
        return post(url, data=json.dumps(params))

    def list(self):
        url = join(self.base_url(), 'tasks')
        return get(url)
