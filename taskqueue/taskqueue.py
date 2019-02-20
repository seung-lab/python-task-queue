from __future__ import print_function

import six

import copy
from functools import partial
import json
import math
import random
import signal
import time
import traceback

import concurrent.futures 
import gevent.pool
import multiprocessing as mp
import numpy as np
from tqdm import tqdm

from cloudvolume.threaded_queue import ThreadedQueue
from cloudvolume.lib import yellow, scatter

from .aws_queue_api import AWSTaskQueueAPI
from .registered_task import RegisteredTask, deserialize
from .scheduler import schedule_green_jobs, schedule_threaded_jobs
from .secrets import (
  PROJECT_NAME, QUEUE_NAME, QUEUE_TYPE,
  AWS_DEFAULT_REGION
)

def totask(task):
  if isinstance(task, RegisteredTask):
    return task

  taskobj = deserialize(task['payload'])
  taskobj._id = task['id']
  return taskobj

class QueueEmpty(LookupError):
  def __init__(self):
    super(LookupError, self).__init__('Queue Empty')

LEASE_SECONDS = 300

class SuperTaskQueue(object):
  def __init__(
    self, queue_name=QUEUE_NAME, queue_server=QUEUE_TYPE, 
    region=None, qurl=None, n_threads=40, project=PROJECT_NAME
  ):
    self._project = project
    self._region = region
    self._queue_name = queue_name
    self._queue_server = queue_server
    self._qurl = qurl
    self._api = self._initialize_interface()

  # This is key to making sure threading works. Don't refactor this method away.
  def _initialize_interface(self):
    server = self._queue_server.lower()
    if server in ('pull-queue', 'google'):
      return NotImplementedError("Google Cloud Tasks are not supported at this time.")
    elif server in ('sqs', 'aws'):
      qurl = self._qurl if self._qurl else self._queue_name
      region = self._region if self._region else AWS_DEFAULT_REGION
      return AWSTaskQueueAPI(qurl=qurl, region_name=region)
    else:
      raise NotImplementedError('Unknown server ' + self._queue_server)

  @property
  def queue_name(self):
    return self._queue_name
  
  @property
  def queue_server(self):
    return self._queue_server

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
      raise QueueEmpty

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

  def poll(
    self, lease_seconds=LEASE_SECONDS, tag=None, 
    verbose=False, execute_args=[], execute_kwargs={}, 
    stop_fn=None, backoff_exceptions=[], min_backoff_window=30, 
    max_backoff_window=120, log_fn=None
  ):
    """
    Poll a queue until a stop condition is reached (default forever). Note
    that this function is not thread safe as it requires a global variable
    to intercept SIGINT.
    lease_seconds: each task should be leased for this many seconds
    tag: if specified, query for only tasks that match this tag
    execute_args / execute_kwargs: pass these arguments to task execution
    backoff_exceptions: A list of exceptions that instead of causing a crash,
      instead cause the polling to back off for an increasing exponential 
      random window.
    min_backoff_window: The minimum sized window (in seconds) to select a 
      random backoff time. 
    max_backoff_window: The window doubles each retry. This is the maximum value
      in seconds.
    stop_fn: A boolean returning function that accepts no parameters. When
      it returns True, the task execution loop will terminate. It is evaluated
      once after every task.
    log_fn: Feed error messages to this function, default print (when verbose is enabled).
    verbose: print out the status of each step
    Return: number of tasks executed
    """
    global LOOP

    if not callable(stop_fn) and stop_fn is not None:
      raise ValueError("stop_fn must be a callable. " + str(stop_fn))
    elif not callable(stop_fn):
      stop_fn = lambda: False

    def random_exponential_window_backoff(n):
      n = min(n, min_backoff_window)
      # 120 sec max b/c on avg a request every ~250msec if 500 containers 
      # in contention which seems like a quite reasonable volume of traffic 
      # to handle
      high = min(2 ** n, max_backoff_window) 
      return random.uniform(0, high)

    def printv(*args, **kwargs):
      if verbose:
        print(*args, **kwargs)

    LOOP = True

    def sigint_handler(signum, frame):
      global LOOP
      printv("Interrupted. Exiting after this task completes...")
      LOOP = False
    
    prev_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)

    if log_fn is None:
      log_fn = printv

    tries = 0
    executed = 0

    backoff = False
    backoff_exceptions = tuple(list(backoff_exceptions) + [ QueueEmpty ])

    while LOOP:
      task = 'unknown' # for error message prior to leasing
      try:
        task = self.lease(seconds=int(lease_seconds))
        tries += 1
        printv(task)
        task.execute(*execute_args, **execute_kwargs)
        executed += 1
        printv("Delete enqueued task...")
        self.delete(task)
        log_fn('INFO', task , "succesfully executed")
        tries = 0
      except backoff_exceptions:
        backoff = True
      except Exception as e:
        printv('ERROR', task, "raised {}\n {}".format(e , traceback.format_exc()))
        raise #this will restart the container in kubernetes
        
      if stop_fn():
        break

      if backoff:
        time.sleep(random_exponential_window_backoff(tries))

      backoff = False

    printv("Task execution loop exited.")
    signal.signal(signal.SIGINT, prev_sigint_handler)

    return executed

  def block_until_empty(self, interval_sec=2):
    while self.enqueued > 0:
      time.sleep(interval_sec)

class TaskQueue(SuperTaskQueue, ThreadedQueue):
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
  def __init__(
    self, queue_name=QUEUE_NAME, queue_server=QUEUE_TYPE, 
    region=None, qurl=None, n_threads=40, project=PROJECT_NAME
  ):

    SuperTaskQueue.__init__(
      self, queue_name, queue_server, 
      region, qurl, n_threads, project
    )
    ThreadedQueue.__init__(self, n_threads) # creates self._queue
    
  def insert(self, task, args=[], kwargs={}, delay_seconds=0):
    """
    Insert a task into an existing queue.
    """
    body = {
      "payload": task.payload(),
      "queueName": self._queue_name,
      "groupByTag": True,
      "tag": task.__class__.__name__
    }

    def cloud_insertion(api):
      api.insert(body, delay_seconds)

    if len(self._threads):
      self.put(cloud_insertion)
    else:
      cloud_insertion(self._api)

    return self

  def insert_all(self, tasks, delay_seconds=0, total=None):
    if total is None:
      try:
        total = len(tasks)
      except TypeError:
        pass
     
    batch_size = 10 
    def genbatches(itr):
      while True:
        batch = []
        try:
          for i in range(batch_size):
            batch.append(next(itr))
        except StopIteration:
          pass

        if len(batch) == 0:
          raise StopIteration

        yield batch

    bodies = (
      {
        "payload": task.payload(),
        "queueName": self._queue_name,
        "groupByTag": True,
        "tag": task.__class__.__name__
      } 

      for task in tasks
    )

    def cloud_insertion(batch):
      self._api.insert(batch, delay_seconds)

    fns = ( partial(cloud_insertion, batch) for batch in genbatches(bodies) )

    schedule_threaded_jobs(
      fns=fns,
      concurrency=20,
      progress='Inserting',
      total=total,
      batch_size=batch_size,
    )

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

class GreenTaskQueue(SuperTaskQueue):
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
  def __init__(
    self, queue_name=QUEUE_NAME, queue_server=QUEUE_TYPE, 
    region=None, qurl=None, n_threads=40, project=PROJECT_NAME
  ):
    n_threads = max(n_threads, 1)
    SuperTaskQueue.__init__(
      self, queue_name, queue_server, 
      region, qurl, n_threads, project
    )
    self._pool = gevent.pool.Pool(n_threads)
    self.check_monkey_patch_status()


  def check_monkey_patch_status(self):
    import gevent.monkey
    if not gevent.monkey.is_module_patched("socket"):
      print(yellow("""
    Switching ThreadedQueue to using green threads. This requires
    monkey patching the standard library to use a cooperative 
    non-blocking threading model.

    Please place the following lines at the beginning of your
    program:

    import gevent.monkey
    gevent.monkey.patch_all()
        """))

  def insert(self, task, args=[], kwargs={}, delay_seconds=0):
    """
    Insert a task into an existing queue.
    """
    body = {
      "payload": task.payload(),
      "queueName": self._queue_name,
      "groupByTag": True,
      "tag": task.__class__.__name__
    }

    def cloud_insertion():
      self._api.insert(body, delay_seconds)

    self._pool.spawn(cloud_insertion)

    return self

  def insert_all(self, tasks, delay_seconds=0, total=None):
    if total is None:
      try:
        total = len(tasks)
      except TypeError:
        pass
     
    batch_size = 10 
    def genbatches(itr):
      while True:
        batch = []
        try:
          for i in range(batch_size):
            batch.append(next(itr))
        except StopIteration:
          pass

        if len(batch) == 0:
          raise StopIteration

        yield batch

    bodies = (
      {
        "payload": task.payload(),
        "queueName": self._queue_name,
        "groupByTag": True,
        "tag": task.__class__.__name__
      } 

      for task in tasks
    )

    def cloud_insertion(batch):
      self._api.insert(batch, delay_seconds)

    fns = ( partial(cloud_insertion, batch) for batch in genbatches(bodies) )

    schedule_green_jobs(
      fns=fns,
      concurrency=20,
      progress='Inserting',
      total=total,
      batch_size=batch_size,
    )

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

  def acknowledge(self, task_id):
    if isinstance(task_id, RegisteredTask):
      task_id = task_id.id

    def cloud_delete():
      self._api.acknowledge(task_id)

    self._pool.spawn(cloud_delete)

    return self

  def delete(self, task_id):
    """Deletes a task from a TaskQueue."""
    if isinstance(task_id, RegisteredTask):
      task_id = task_id.id

    def cloud_delete():
      self._api.delete(task_id)

    self._pool.spawn(cloud_delete)

    return self

  def wait(self):
    self._pool.join()

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self._pool.join()

class MockTaskQueue(object):
  def __init__(self, *args, **kwargs):
    pass

  def insert(self, task, args=[], kwargs={}):
    task = {
      'payload': task.payload(),
      'id': -1,
    }
    task = totask(task) # to ensure conformity with TaskQueue
    task.execute(*args, **kwargs)
    del task

  def insert_all(self, tasks, args=[], kwargs={}, delay_seconds=0, total=None):
    for task in tasks:
      self.insert(task, args=args, kwargs=kwargs)

  def poll(self, *args, **kwargs):
    return self

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

  def insert(self, task, args=[], kwargs={}):
    # task.payload to ensure conformity with TaskQueue
    task = {
      'payload': task.payload(),
      'id': -1,
    }

    self.queue.append( (task, args, kwargs) )

  def insert_all(
      self, tasks, args=[], kwargs={}, 
      delay_seconds=0, total=None
    ):
    for task in tasks:
      self.queue.append( (task, args, kwargs) )

  def wait(self, progress=None):
    self._process(progress)
    return self

  def poll(self, *args, **kwargs):
    pass

  def kill_threads(self):
    return self

  def _process(self, progress=True):
    with tqdm(total=len(self.queue), desc="Tasks", disable=(not progress)) as pbar:
      if self.parallel == 1:
        while self.queue:
          _task_execute(self.queue.pop(0))
          pbar.update()
      else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.parallel) as executor:
          for _ in executor.map(_task_execute, self.queue):
            pbar.update()
    
      self.queue = []

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self._process()

# Necessary to define here to make the 
# function picklable
def _task_execute(task_tuple):
  task, args, kwargs = task_tuple
  task = totask(task)
  task.execute(*args, **kwargs)

## Multiprocess Upload

def _scatter(sequence, n):
  """Scatters elements of ``sequence`` into ``n`` blocks."""

  output = []
  chunklen = int(math.ceil(float(len(sequence)) / float(n)))

  for i in range(n):
    output.append(
      copy.deepcopy(sequence[i*chunklen:(i+1)*chunklen])
    )

  return output

def _upload_tasks_single_process(queue_name, tasks):
  with TaskQueue(queue_name=queue_name, queue_server='sqs') as tq:
    for task in tasks:
      tq.insert(task)

def upload(queue_name, tasks, parallel=16, progress=False):
  if parallel == 0:
    parallel = mp.cpu_count()
  elif parallel < 0:
    raise ValueError("Parallel must be a positive number or zero (all cpus). Got: " + str(parallel))

  parallel = max(1, min(parallel, mp.cpu_count()))

  if parallel == 1:
    _upload_tasks_single_process(queue_name, tasks)
  else:
    uploadfn = partial(_upload_tasks_single_process, queue_name)
    tasks = _scatter(tasks, parallel)

    with concurrent.futures.ProcessPoolExecutor(max_workers=parallel) as executor:
      executor.map(uploadfn, tasks)



