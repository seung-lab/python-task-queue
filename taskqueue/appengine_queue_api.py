import json
import urllib

from httplib2 import Http
from googleapiclient.http import HttpRequest

from .secrets import APPENGINE_QUEUE_URL

class AppEngineTaskQueue():

    class Tasks():
        def __init__(self, appengine):
            self._ae = appengine

        def _postprocess(self, response, content):
            if content:
                content = json.loads(content)

            if type(content) == list and len(content):
                # mimicking  Google's Pull Queue API
                return {'items': content} 

            return content

        def insert(self,  project, taskqueue, body):

            if 'groupByTag' not in body or not body['groupByTag']:
                body['tag'] = ''
            
            if 'groupByTag' in body: del body['groupByTag']
            if 'queueName' in body: del body['queueName']

            return HttpRequest(
                self._ae._http,
                postproc=self._postprocess,
                uri=self._ae._queue_url + '/{}/taskqueue/{}/tasks'.format(
                    project, taskqueue),
                method='POST',
                body=json.dumps(body))

        def get(self, project, taskqueue, task):
            return HttpRequest(
                self._ae._http,
                postproc=self._postprocess,
                uri=self._ae._queue_url + '/{}/taskqueue/{}/tasks/{}'.format(
                    project, taskqueue, task),
                method='GET')

        def delete(self, project, taskqueue, task):
            return HttpRequest(
                self._ae._http,
                postproc=self._postprocess,
                uri=self._ae._queue_url + '/{}/taskqueue/{}/tasks/{}'.format(
                    project, taskqueue, task),
                method='DELETE')


        def lease(self, project, taskqueue, numTasks=1, leaseSecs=1, groupByTag=False, tag=''):

            if groupByTag == False:
                tag = ''
            query_str = '?' + urllib.urlencode(
                {'numTasks':numTasks, 'leaseSecs':leaseSecs, 'tag':tag})

            return HttpRequest(
                self._ae._http,
                postproc=self._postprocess,
                uri=self._ae._queue_url + '/{}/taskqueue/{}/tasks/lease{}'.format(
                    project, taskqueue, query_str),
                method='POST')

        def list(self, project, taskqueue):
            return HttpRequest(
                self._ae._http,
                postproc=self._postprocess,
                uri=self._ae._queue_url + '/{}/taskqueue/{}/tasks'.format(
                    project, taskqueue),
                method='GET')


    class TaskQueues():
        def __init__(self, appengine):
            self._ae = appengine

        def _postprocess(self, response, content):
            if content:
                content = json.loads(content)

            if type(content) == list and len(content):
                # mimicking  Google's Pull Queue API
                return {'items': content} 

            return content

        def get(self, project, taskqueue, getStats=False):
            return HttpRequest(
                self._ae._http,
                postproc=self._postprocess,
                uri=self._ae._queue_url + '/{}/taskqueue/{}'.format(
                    project, taskqueue),
                method='GET')            

    def __init__(self):
        self._queue_url = APPENGINE_QUEUE_URL
        self._http = Http()  
        self._tasks = AppEngineTaskQueue.Tasks(self)
        self._taskqueues = AppEngineTaskQueue.TaskQueues(self)

    def tasks(self):
        return self._tasks

    def taskqueues(self):
        return self._taskqueues