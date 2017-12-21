from __future__ import print_function

import json
import inspect
import base64
import copy
import re
from collections import OrderedDict
from functools import partial

import googleapiclient.errors
import googleapiclient.discovery
import numpy as np

from future.utils import with_metaclass

from cloudvolume.threaded_queue import ThreadedQueue
from .secrets import google_credentials, PROJECT_NAME, QUEUE_NAME, QUEUE_TYPE
from .appengine_queue_api import AppEngineTaskQueue

__all__ = [ 'RegisteredTask', 'TaskQueue' ]

registry = {}

def register_class(target_class):
    registry[target_class.__name__] = target_class

def deserialize(data):
    params = json.loads(data)
    name = params['class']
    target_class = registry[name]
    del params['class']
    return target_class(**params)

def payloadBase64Decode(payload):
    decoded_string = base64.b64decode(payload).encode('ascii')
    return deserialize(decoded_string)

class Meta(type):
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        register_class(cls)
        cls._arg_names = inspect.getargspec(class_dict['__init__'])[0][1:]
        return cls

class RegisteredTask(with_metaclass(Meta)):
    def __init__(self, *arg_values):
        self._args = OrderedDict(zip(self._arg_names, arg_values))

    @classmethod
    def deserialize(cls, base64data):
        obj = deserialize(base64data)
        assert type(obj) == cls
        return obj
        
    def serialize(self):
        d = copy.deepcopy(self._args)
        d['class'] = self.__class__.__name__

        def serializenumpy(obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj

        return json.dumps(d, default=serializenumpy)

    @property
    def payloadBase64(self):
        return base64.b64encode(self.serialize().encode('utf8'))

    @property
    def id(self):
        return self._id

    def __repr__(self):
        
        string = self.__class__.__name__ + "("
        for arg_name, arg_value in self._args.iteritems():
            if type(arg_value) is str or type(arg_value) is unicode:
                string += "{}='{}',".format(arg_name, arg_value)
            else:
                string += "{}={},".format(arg_name, arg_value)

        # remove the last comma if necessary
        if string[-1] == ',':
            string = string[:-1]

        return string + ")"  

class TaskQueue(ThreadedQueue):
    """
    The standard usage is that a client calls lease to get the next available task,
    performs that task, and then calls task.delete on that task before the lease expires.
    If the client cannot finish the task before the lease expires,
    and has a reasonable chance of completing the task,
    it should call task.update before the lease expires.

    If the client completes the task after the lease has expired,
    it still needs to delete the task. 

    Tasks should be designed to be idempotent to avoid errors 
    if multiple clients complete the same task.
    """
    class QueueEmpty(LookupError):
        def __init__(self):
            super(LookupError, self).__init__('Queue Empty')

    def __init__(self, n_threads=40, project=PROJECT_NAME,
                 queue_name=QUEUE_NAME, queue_server=QUEUE_TYPE):

        self._project = 's~' + project # s~ means North America, e~ means Europe
        self._queue_name = queue_name
        self._queue_server = queue_server
        self._api = self._initialize_interface()

        super(TaskQueue, self).__init__(n_threads) # creates self._queue

    # This is key to making sure threading works. Don't refactor this method away.
    def _initialize_interface(self):
        if self._queue_server == 'appengine':
            return AppEngineTaskQueue()
        elif self._queue_server == 'pull-queue':
            return googleapiclient.discovery.build('taskqueue', 'v1beta2', credentials=google_credentials)
        else:
            raise ValueError('Unkown server ' + self._queue_server)

    @property
    def enqueued(self):
        """
        Returns the approximate(!) number of tasks enqueued in the cloud.

        WARNING: The number computed by Google is eventually
            consistent. It may return impossible numbers that
            are small deviations from the number in the queue.
            For instance, we've seen 1005 enqueued after 1000 
            inserts.
        
        Returns: (int) number of tasks in cloud queue
        """
        tqinfo = self.get()
        return tqinfo['stats']['totalTasks']
        
    def _consume_queue_execution(self, fn):
        try:
            super(self.__class__, self)._consume_queue_execution(fn)
        except googleapiclient.errors.HttpError as httperr:
            # Retry if Timeout, Service Unavailable, or ISE
            # ISEs can happen from flooding or other issues that
            # aren't the fault of the request.
            if httperr.resp.status in (408, 500, 503): 
                self.put(fn)
            elif httperr.resp.status == 400:
                if not re.search('task name is invalid', repr(httperr.content), flags=re.IGNORECASE):
                    raise
            else:
                raise

    def insert(self, task):
        """
        Insert a task into an existing queue.
        """
        body = {
            "payloadBase64": task.payloadBase64.decode('utf8'),
            "queueName": self._queue_name,
            "groupByTag": True,
            "tag": task.__class__.__name__
        }

        def cloud_insertion(api):
            api.tasks().insert(
                project=self._project,
                taskqueue=self._queue_name,
                body=body,
            ).execute(num_retries=6)

        if len(self._threads):
            self.put(cloud_insertion)
        else:
            cloud_insertion(self._api)

        return self

    def get(self):
        """
        Gets information about the TaskQueue
        """
        return self._api.taskqueues().get(
            project=self._project,
            taskqueue=self._queue_name,
            getStats=True,
        ).execute(num_retries=6)

    def get_task(self, tid):
        """
        Gets the named task in the TaskQueue. 
        tid is a unique string Google provides 
        e.g. '7c6e81c9b7ab23f0'
        """
        return self._api.tasks().get(
            project=self._project,
            taskqueue=self._queue_name,
            task=tid,
        ).execute(num_retries=6)

    def list(self):
        """
        Lists all non-deleted Tasks in a TaskQueue, 
        whether or not they are currently leased, up to a maximum of 100.
        """
        return self._api.tasks().list(
            project=self._project, 
            taskqueue=self._queue_name
        ).execute(num_retries=6)

    def update(self, task):
        """
        Update the duration of a task lease.
        Required query parameters: newLeaseSeconds
        """
        raise NotImplemented

    def lease(self, tag=None):
        """
        Acquires a lease on the topmost N unowned tasks in the specified queue.
        Required query parameters: leaseSecs, numTasks
        """
        tag = tag if tag else None
        
        tasks = self._api.tasks().lease(
            project=self._project,
            taskqueue=self._queue_name, 
            numTasks=1, 
            leaseSecs=600,
            groupByTag=(tag is not None),
            tag=tag,
        ).execute(num_retries=6)

        if not 'items' in tasks:
            raise TaskQueue.QueueEmpty
          
        task_json = tasks['items'][0]
        t = payloadBase64Decode(task_json['payloadBase64'])
        t._id =  task_json['id']
        return t

    def patch(self):
        """
        Update tasks that are leased out of a TaskQueue.
        Required query parameters: newLeaseSeconds
        """
        raise NotImplemented

    def purge(self):
        """Deletes all tasks in the queue."""
        while True:
            lst = self.list()
            if 'items' not in lst:
                break

            for task in lst['items']:
                self.delete(task['id'])
            self.wait()
        return self

    def delete(self, task_id):
        """Deletes a task from a TaskQueue."""
        if isinstance(task_id, RegisteredTask):
            task_id = task_id.id

        def cloud_delete(api):
            api.tasks().delete(
                project=self._project,
                taskqueue=self._queue_name,
                task=task_id,
            ).execute(num_retries=6)

        if len(self._threads):
            self.put(cloud_delete)
        else:
            cloud_delete(self._api)
        return self

class MockTaskQueue():
    def __init__(self, queue_name='', queue_server=''):
        pass

    def insert(self, task):
        task.execute()
        del task

    def wait(self, progress=None):
      return self

    def kill_threads(self):
      return self

    def __enter__(self):
      return self

    def __exit__(self, exception_type, exception_value, traceback):
      pass
