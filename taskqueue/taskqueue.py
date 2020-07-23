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

import gevent.pool
import multiprocessing as mp
import numpy as np
import pathos.pools
from tqdm import tqdm

from .threaded_queue import ThreadedQueue
from .lib import yellow, scatter, sip, toiter

from .aws_queue_api import AWSTaskQueueAPI
from .paths import extract_path, mkpath
from .registered_task import RegisteredTask, deserialize
from .scheduler import schedule_jobs
from .secrets import (
  AWS_DEFAULT_REGION
)

AWS_BATCH_SIZE = 10 

def totask(task):
  if isinstance(task, RegisteredTask):
    return task

  taskobj = deserialize(task['payload'])
  taskobj._id = task['id']
  return taskobj

def totaskid(taskid):
  if isinstance(taskid, RegisteredTask):
    return taskid.id
  return taskid

def totalfn(iterator, total):
  if total is not None:
    return total
  try:
    return len(iterator)
  except TypeError:
    return None

class QueueEmptyError(LookupError):
  pass

LEASE_SECONDS = 300

class TaskQueue(object):
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
    self, qurl, n_threads=40, 
    green=False, progress=True, 
    **kwargs
  ):
    self.qurl = qurl
    self.path = extract_path(qurl)
    self.api = self.initialize_api(self.path, kwargs)
    self.n_threads = n_threads
    self.green = bool(green)
    self.progress = bool(progress),
    self.kwargs = kwargs

    if self.green:
      self.check_monkey_patch_status()

  def initialize_api(self, path, kwargs):
    if path.protocol == 'sqs':
      return AWSTaskQueueAPI(qurl=path.path, region_name=kwargs.get('region', AWS_DEFAULT_REGION))
    # elif path.protocol == 'fq':
    #   return FileQueueAPI(...)
    else:
      raise ValueError('Unsupported protocol ' + str(self.path.protocol))

  def check_monkey_patch_status(self):
    import gevent.monkey
    if not gevent.monkey.is_module_patched("socket"):
      print(yellow("""
    Green threads require monkey patching the standard library 
    to use a non-blocking network socket call.

    Please place the following lines at the beginning of your
    program. `thread=False` is there because sometimes this
    causes hanging in multiprocessing.

    import gevent.monkey
    gevent.monkey.patch_all(thread=False)
        """))

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
    return self.api.enqueued

  # def status(self):
  #   """
  #   Gets information about the TaskQueue
  #   """
  #   return self.api.get(getStats=True)

  def list(self):
    """
    Lists all non-deleted Tasks in a TaskQueue, 
    whether or not they are currently leased, 
    up to a maximum of 100.
    """
    return [ totask(x) for x in self.api.list() ]

  def insert(self, tasks, delay_seconds=0, total=None, parallel=1):
    total = totalfn(tasks, total)

    if parallel not in (1, False) and total is not None and total > 1:
      multiprocess_upload(self.__class__, mkpath(self.path), tasks, parallel=parallel)
      return

    batch_size = AWS_DEFAULT_REGION
    
    schedule_jobs(
      fns=self._gen_insert_all_tasks(
        tasks, batch_size, delay_seconds, total
      ),
      concurrency=self.n_threads,
      progress='Inserting',
      total=total,
      green=self.green,
    )

  def insert_all(self, *args, **kwargs):
    """For backwards compatibility."""
    return self.insert(*args, **kwargs)

  def _gen_insert_all_tasks(
      self, tasks, 
      batch_size=AWS_BATCH_SIZE, 
      delay_seconds=0, 
      total=None
    ):
    bodies = (
      {
        "payload": task.payload(),
        "queueName": self.path.path,
        "groupByTag": True,
        "tag": task.__class__.__name__
      } 

      for task in tasks
    )

    return ( partial(self.api.insert, batch, delay_seconds) for batch in sip(bodies, AWS_BATCH_SIZE) )

  def renew(self, task, seconds):
    """Update the duration of a task lease."""
    return self.api.renew_lease(task, seconds)

  def cancel(self, task):
    return self.api.cancel_lease(task)

  def lease(self, seconds=600, num_tasks=1, tag=None):
    """
    Acquires a lease on the topmost N unowned tasks in the specified queue.
    Required query parameters: leaseSecs, numTasks
    """
    tag = tag if tag else None
    tasks = self.api.lease(
      numTasks=num_tasks, 
      seconds=seconds,
      groupByTag=(tag is not None),
      tag=tag,
    )

    if not len(tasks):
      raise QueueEmptyError()

    task = tasks[0]
    return totask(task)

  def delete(self, task_id, total=None):
    """Deletes a task from a TaskQueue."""
    task_id = toiter(task_id)
    total = totalfn(task_id, total)

    def deltask(tid):
      self.api.delete(totaskid(tid))

    schedule_jobs(
      fns=( partial(deltask, tid) for tid in task_id ),
      concurrency=self.n_threads,
      progress=('Deleting' if self.progress else None),
      total=total,
      green=self.green,
    )

  def purge(self):
    """Deletes all tasks in the queue."""
    try:
      return self.api.purge()
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
    max_backoff_window=120, before_fn=None, after_fn=None
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
    before_fn: Pass task pre-execution.
    after_fn: Pass task post-execution.
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

    tries = 0
    executed = 0

    backoff = False
    backoff_exceptions = tuple(list(backoff_exceptions) + [ QueueEmptyError ])

    before_fn = before_fn or (lambda x: x)
    after_fn = after_fn or (lambda x: x)

    while LOOP:
      task = 'unknown' # for error message prior to leasing
      try:
        task = self.lease(seconds=int(lease_seconds))
        tries += 1
        before_fn(task)
        time_start = time.time()
        task.execute(*execute_args, **execute_kwargs)
        time_delta = time.time() - time_start
        executed += 1
        printv("Delete enqueued task...")
        self.delete(task)
        printv('INFO', task , "succesfully executed in {:.2f} sec.".format(time_delta))
        after_fn(task)
        tries = 0
      except backoff_exceptions:
        backoff = True
      except Exception as e:
        printv('ERROR', task, "raised {}\n {}".format(e , traceback.format_exc()))
        raise # this will restart the container in kubernetes
        
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


class LocalTaskQueue(object):
  def __init__(self, parallel=1, queue_name='', queue_server='', progress=True):
    if parallel and type(parallel) == bool:
      parallel = mp.cpu_count()

    self.parallel = parallel
    self.queue = []
    self.progress = progress

  def insert(
      self, tasks, 
      delay_seconds=0, total=None,
      parallel=None, progress=True
    ):
    for task in tasks:
      args, kwargs = [], {}
      if isinstance(task, tuple):
        task, args, kwargs = task
      task = {
        'payload': task.payload(),
        'id': -1,
      }
      self.queue.append( (task, args, kwargs) )

    self.execute(progress, parallel, total)

  def insert_all(self, *args, **kwargs):
    return self.insert(*args, **kwargs)

  def poll(self, *args, **kwargs):
    pass

  def execute(self, progress=True, parallel=None, total=None):
    if parallel is None:
      parallel = self.parallel

    total = totalfn(self.queue, total)

    with tqdm(total=total, desc="Tasks", disable=(not progress)) as pbar:
      if self.parallel == 1:
        while self.queue:
          _task_execute(self.queue.pop(0))
          pbar.update()
      else:
        with pathos.pools.ProcessPool(self.parallel) as executor:
          for _ in executor.imap(_task_execute, self.queue):
            pbar.update()
    
      self.queue = []

class MockTaskQueue(LocalTaskQueue):
  pass

# Necessary to define here to make the 
# function picklable
def _task_execute(task_tuple):
  task, args, kwargs = task_tuple
  task = totask(task)
  task.execute(*args, **kwargs)

## Multiprocess Upload

def _scatter(sequence, n):
  """Scatters elements of ``sequence`` into ``n`` blocks."""
  chunklen = int(math.ceil(float(len(sequence)) / float(n)))

  return [
    sequence[ i*chunklen : (i+1)*chunklen ] for i in range(n)
  ]

def soloprocess_upload(QueueClass, queue_name, tasks):
  tq = QueueClass(queue_name)
  tq.insert(tasks)

error_queue = mp.Queue()

def multiprocess_upload(QueueClass, queue_name, tasks, parallel=True):
  if parallel is True:
    parallel = mp.cpu_count()
  elif parallel <= 0:
    raise ValueError("Parallel must be a positive number or zero (all cpus). Got: " + str(parallel))

  if parallel == 1:
    soloprocess_upload(QueueClass, queue_name, tasks)
    return 

  def capturing_soloprocess_upload(*args, **kwargs):
    try:
      soloprocess_upload(*args, **kwargs)
    except Exception as err:
      print(err)
      error_queue.put(err)

  uploadfn = partial(
    capturing_soloprocess_upload, QueueClass, queue_name
  )
  tasks = _scatter(tasks, parallel)

  # This is a hack to get dill to pickle dynamically
  # generated classes. This is an important use case
  # for when we create iterators with generator __iter__
  # functions on demand.

  # https://github.com/uqfoundation/dill/issues/56

  try:
    task = next(item for item in tasks if item is not None)
  except StopIteration:
    return 

  cls_module = task.__class__.__module__
  task.__class__.__module__ = '__main__'

  with pathos.pools.ProcessPool(parallel) as pool:
    pool.map(uploadfn, tasks)

  task.__class__.__module__ = cls_module

  if not error_queue.empty():
    errors = []
    while not error_queue.empty():
      err = error_queue.get()
      if err is not StopIteration:
        errors.append(err)
    if len(errors):
      raise Exception(errors)



