from __future__ import print_function

import six
import json
from functools import partial

import concurrent.futures 
import multiprocessing as mp
import googleapiclient.errors
import numpy as np
from tqdm import tqdm

from cloudvolume.threaded_queue import ThreadedQueue

from .appengine_queue_api import AppEngineTaskQueueAPI
from .google_queue_api import GoogleTaskQueueAPI
from .aws_queue_api import AWSTaskQueueAPI
from .registered_task import RegisteredTask, payloadBase64Decode
from .secrets import PROJECT_NAME, QUEUE_NAME, QUEUE_TYPE

def totask(task):
    taskobj = payloadBase64Decode(task['payloadBase64'])
    taskobj._id = task['id']
    return taskobj

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

    def __init__(self, n_threads=40, project=PROJECT_NAME, region=None,
                 queue_name=QUEUE_NAME, queue_server=QUEUE_TYPE, qurl=None):

        self._project = project
        self._region = region
        self._queue_name = queue_name
        self._queue_server = queue_server
        self._qurl = qurl
        self._api = self._initialize_interface()

        super(TaskQueue, self).__init__(n_threads) # creates self._queue

    # This is key to making sure threading works. Don't refactor this method away.
    def _initialize_interface(self):
        server = self._queue_server.lower()
        if server == 'appengine':
            return AppEngineTaskQueueAPI(project=self._project, queue_name=self._queue_name)
        elif server in ('pull-queue', 'google'):
            return GoogleTaskQueueAPI(project=self._project, queue_name=self._queue_name)
        elif server in ('sqs', 'aws'):
            return AWSTaskQueueAPI(qurl=self._qurl)
        else:
            raise NotImplementedError('Unknown server ' + self._queue_server)

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
        return self._api.enqueued
        
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
            api.insert(body)

        if len(self._threads):
            self.put(cloud_insertion)
        else:
            cloud_insertion(self._api)

        return self

    def status(self):
        """
        Gets information about the TaskQueue
        """
        return self._api.get(getStats=True)

    def get_task(self, tid):
        """
        Gets the named task in the TaskQueue. 
        tid is a unique string Google provides 
        e.g. '7c6e81c9b7ab23f0'
        """
        return self._api.get(tid)

    def list(self):
        """
        Lists all non-deleted Tasks in a TaskQueue, 
        whether or not they are currently leased, up to a maximum of 100.
        """
        return [ totask(x) for x in self._api.list() ]

    def renew_lease(self, task, seconds):
        """Update the duration of a task lease."""
        return self._api.renew_lease(task, seconds)

    def cancel_lease(self, task):
        return self._api.cancel_lease(task)

    def lease(self, seconds=600, num_tasks=1, tag=None):
        """
        Acquires a lease on the topmost N unowned tasks in the specified queue.
        Required query parameters: leaseSecs, numTasks
        """
        tag = tag if tag else None
        tasks = self._api.lease(
            numTasks=num_tasks, 
            seconds=seconds,
            groupByTag=(tag is not None),
            tag=tag,
        )

        if not len(tasks):
            raise TaskQueue.QueueEmpty

        task = tasks[0]
        return totask(task)

    def patch(self):
        """
        Update tasks that are leased out of a TaskQueue.
        Required query parameters: newLeaseSeconds
        """
        raise NotImplemented

    def purge(self):
        """Deletes all tasks in the queue."""
        try:
            return self._api.purge()
        except AttributeError:
            while True:
                lst = self.list()
                if len(lst) == 0:
                    break

                for task in lst:
                    self.delete(task)
                self.wait()
            return self

    def acknowledge(self, task_id):
        if isinstance(task_id, RegisteredTask):
            task_id = task_id.id

        def cloud_delete(api):
            api.acknowledge(task_id)

        if len(self._threads):
            self.put(cloud_delete)
        else:
            cloud_delete(self._api)

        return self

    def delete(self, task_id):
        """Deletes a task from a TaskQueue."""
        if isinstance(task_id, RegisteredTask):
            task_id = task_id.id

        def cloud_delete(api):
            api.delete(task_id)

        if len(self._threads):
            self.put(cloud_delete)
        else:
            cloud_delete(self._api)

        return self

class MockTaskQueue(object):
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

class LocalTaskQueue(object):
    def __init__(self, parallel=1, queue_name='', queue_server='', progress=True):
        if parallel and type(parallel) == bool:
            parallel = mp.cpu_count()

        self.parallel = parallel
        self.queue = []
        self.progress = progress

    def insert(self, task):
        self.queue.append(task)

    def wait(self, progress=None):
      return self

    def kill_threads(self):
      return self

    def __enter__(self):
      return self

    def __exit__(self, exception_type, exception_value, traceback):
        with tqdm(total=len(self.queue), desc="Tasks") as pbar:
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.parallel) as executor:
                for _ in executor.map(task_execute, self.queue):
                    pbar.update()
        self.queue = []

# Necessary to define here to make the 
# function picklable
def task_execute(task):
    task.execute()
