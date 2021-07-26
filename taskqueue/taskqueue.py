import copy
from functools import partial
import itertools
import json
import math
import os
import platform
import random
import signal
import threading
import time
import traceback
import types
import sys

import gevent.pool
import multiprocessing as mp
import numpy as np
import pathos.pools
from tqdm import tqdm

from .threaded_queue import ThreadedQueue
from .lib import yellow, scatter, sip, toiter

from .aws_queue_api import AWSTaskQueueAPI, AWS_BATCH_SIZE
from .file_queue_api import FileQueueAPI
from .paths import extract_path, mkpath
from .scheduler import schedule_jobs
from .queueables import totask, totaskid
from .queueablefns import FunctionTask

def totalfn(iterator, total):
  if total is not None:
    return total
  try:
    return len(iterator)
  except TypeError:
    return None

class UnsupportedProtocolError(BaseException):
  pass

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
  
  The kwargs parameter dict should be queue-type specific parameters that are needed.
  """
  def __init__(
    self, qurl, n_threads=40, 
    green=False, progress=True, 
    **kwargs
  ):
    self.qurl = qurl
    self.path = extract_path(qurl)
    self.api = self.initialize_api(self.path, **kwargs)
    self.n_threads = n_threads
    self.green = bool(green)
    self.progress = bool(progress)

    if self.green:
      self.check_monkey_patch_status()

  @property
  def qualified_path(self):
    return mkpath(self.path)

  def initialize_api(self, path, **kwargs):
    """Creates correct API object for the type of path 
    
    Args:
      path: ExtractedPath representing the location of the queue
      region_name: The region for cloud-based queues (optional)
      kwargs: Keywords to be passed to the underlying queue (optional)
    """
    if path.protocol == 'sqs':
      return AWSTaskQueueAPI(path.path, **kwargs)
    elif path.protocol == 'fq':
      return FileQueueAPI(path.path)
    else:
      raise UnsupportedProtocolError('Unsupported protocol ' + str(self.path.protocol))

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

  @property
  def inserted(self):
    return self.api.inserted

  @property
  def completed(self):
    return self.api.completed

  @property
  def leased(self):
    return self.api.leased

  def is_empty(self):
    return self.api.is_empty()

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
    return [ totask(x) for x in iter(self.api) ]

  def insert(
    self, tasks, delay_seconds=0, 
    total=None, parallel=1, skip_insert_counter=False
  ):
    """Inserts tasks and returns number inserted."""
    if isinstance(tasks, TaskQueue):
      taskgen = tasks.tasks()
      if not isinstance(taskgen, TaskQueue):
        return self.insert(taskgen, delay_seconds, total, parallel)
      else:
        raise ValueError(str(tasks) + " would have caused an infinite recursion by returning a TaskQueue object from obj.tasks()")
    
    tasks = toiter(tasks)
    total = totalfn(tasks, total)

    if parallel not in (1, False) and total is not None and total > 1:
      return multiprocess_upload(self.__class__, mkpath(self.path), tasks, parallel=parallel, total=total)

    try:
      batch_size = self.api.batch_size
    except:
      batch_size = 1
    
    bodies = (
      {
        "payload": totask(task).payload(),
        "queueName": self.path.path,
      } 
      for task in tasks
    )

    def insertfn(batch):
      return self.api.insert(batch, delay_seconds) 
    
    cts = schedule_jobs(
      fns=( partial(insertfn, batch) for batch in sip(bodies, batch_size) ),
      concurrency=self.n_threads,
      progress=('Inserting' if self.progress else False),
      total=total,
      green=self.green,
    )
    cts = sum(cts)

    if not skip_insert_counter:
      self.api.add_insert_count(cts)
    return cts

  def add_insert_count(self, ct):
    self.api.add_insert_count(ct)

  def insert_all(self, *args, **kwargs):
    """For backwards compatibility."""
    return self.insert(*args, **kwargs)

  def rezero(self):
    """Resets statistic counters such as completions and insertions to zero."""
    self.api.rezero()

  def renew(self, task, seconds):
    """Update the duration of a task lease."""
    return self.api.renew_lease(task, seconds)

  def cancel(self, task):
    return self.api.cancel_lease(task)

  def release_all(self):
    return self.api.release_all()

  def lease(self, seconds=600, num_tasks=1, wait_sec=None):
    """
    Acquires a lease on the topmost N unowned tasks in the specified queue.
    Required query parameters: leaseSecs, numTasks
    """
    if num_tasks <= 0:
      raise ValueError("num_tasks must be > 0. Got: " + str(num_tasks))
    if seconds < 0:
      raise ValueError("lease seconds must be >= 0. Got: " + str(seconds))

    tasks = self.api.lease(seconds, num_tasks, wait_sec)

    if not len(tasks):
      raise QueueEmptyError()

    if num_tasks == 1:
      return totask(tasks[0])
    else:
      return [ totask(task) for task in tasks ]

  def delete(self, task_id, total=None, tally=False):
    """Deletes a task from a TaskQueue."""
    task_id = toiter(task_id)
    total = totalfn(task_id, total)

    def deltask(tid):
      num_deleted = self.api.delete(totaskid(tid))
      if tally:
        self.api.tally()
      return num_deleted

    schedule_jobs(
      fns=( partial(deltask, tid) for tid in task_id ),
      concurrency=self.n_threads,
      progress=('Deleting' if self.progress else None),
      total=total,
      green=self.green,
    )

  def purge(self, native=False):
    """Deletes all tasks in the queue."""
    try:
      return self.api.purge(native)
    except AttributeError:
      while True:
        lst = self.list()
        if len(lst) == 0:
          break

        for task in lst:
          self.delete(task)
        self.wait()
      return self

  def tasks(self):
    """
    Iterate over all tasks.

    Can cause infinite loops on SQS and so is not
    supported. You can use the api method directly 
    if you know what you're doing.
    """
    if self.path.protocol == "sqs":
      raise UnsupportedProtocolError("SQS could enter an infinite loop from this method.")

    return ( totask(task) for task in iter(self.api) )

  def poll(
    self, lease_seconds=LEASE_SECONDS,  
    verbose=False, execute_args=[], execute_kwargs={}, 
    stop_fn=None, backoff_exceptions=[], min_backoff_window=1, 
    max_backoff_window=120, before_fn=None, after_fn=None,
    tally=False
  ):
    """
    Poll a queue until a stop condition is reached (default forever). Note
    that this function is not thread safe as it requires a global variable
    to intercept SIGINT.
    lease_seconds: each task should be leased for this many seconds
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
      once after every task. If you provide the arguments `executed` (tasks completed) 
      `tries` (current attempts at fetching a task), `previous_execution_time` (time in
      seconds to run the last task), or `elapsed_time` (time since polling started in 
      seconds) they will be dependency injected.
    before_fn: Pass task pre-execution.
    after_fn: Pass task post-execution.
    verbose: print out the status of each step
    tally: contribute each completed task to a completions counter if supported.

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
      if LOOP:
        print("Interrupted. Exiting after this task completes. Press Ctrl-C again to exit now.", flush=True)
        LOOP = False
      else:
        sys.exit()
    
    prev_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)

    tries = 0
    executed = 0
    total_elapsed_sec = 0

    backoff = False
    backoff_exceptions = tuple(list(backoff_exceptions) + [ QueueEmptyError ])

    before_fn = before_fn or (lambda x: x)
    after_fn = after_fn or (lambda x: x)

    loop_init_time = time.time()

    while LOOP:
      total_elapsed_sec = time.time() - loop_init_time
      task = 'unknown' # for error message prior to leasing
      try:
        task = self.lease(seconds=int(lease_seconds))
        tries += 1
        before_fn(task)
        printv("INFO Running", task, " (id: {})".format(task.id))
        time_start = time.time()
        task.execute(*execute_args, **execute_kwargs)
        time_delta = time.time() - time_start
        executed += 1
        printv("INFO Deleting", task.id)
        self.delete(task, tally=tally)
        printv('INFO', type(task).__name__, task.id , "succesfully executed in {:.2f} sec.".format(time_delta))
        after_fn(task)
        tries = 0
      except backoff_exceptions:
        backoff = True
      except Exception as e:
        printv('ERROR', task, "raised {}\n {}".format(e , traceback.format_exc()))
        raise # this will restart the container in kubernetes
      
      varnames = stop_fn.__code__.co_varnames
      stop_fn_bound = stop_fn
      if 'executed' in varnames:
        stop_fn_bound = partial(stop_fn_bound, executed=executed)
      if 'tries' in varnames:
        stop_fn_bound = partial(stop_fn_bound, tries=tries)
      if 'previous_execution_time' in varnames:
        stop_fn_bound = partial(stop_fn_bound, previous_execution_time=time_delta) 
      if 'elapsed_time' in varnames:
        stop_fn_bound = partial(stop_fn_bound, elapsed_time=total_elapsed_sec) 

      if stop_fn_bound():
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

  def insert(
      self, tasks, 
      delay_seconds=0, total=None,
      parallel=None, progress=True
    ):
    tasks = toiter(tasks)
    ct = 0
    for task in tasks:
      args, kwargs = [], {}
      if isinstance(task, tuple):
        task, args, kwargs = task
      task = totask(task)
      task = {
        'payload': task.payload(),
        'id': -1,
      }
      self.queue.append( (task, args, kwargs) )
      ct += 1

    return ct

  def insert_all(self, *args, **kwargs):
    ct = self.insert(*args, **kwargs)
    self.execute(self.progress)
    return ct

  def add_insert_count(self, ct):
    pass

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

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.execute()

class MockTaskQueue(LocalTaskQueue):
  pass

class GreenTaskQueue(TaskQueue):
  def __init__(self, *args, **kwargs):
    kwargs['green'] = True
    super(GreenTaskQueue, self).__init__(*args, **kwargs)

# Necessary to define here to make the 
# function picklable
def _task_execute(task_tuple):
  task, args, kwargs = task_tuple
  task = totask(task)
  task.execute(*args, **kwargs)

## Multiprocess Upload

def soloprocess_upload(QueueClass, queue_name, tasks):
  tq = QueueClass(queue_name, progress=False)
  return tq.insert(tasks, skip_insert_counter=True)

error_queue = mp.Queue()

def multiprocess_upload(QueueClass, queue_name, tasks, parallel=True, total=None):
  if parallel is True:
    parallel = mp.cpu_count()
  elif parallel <= 0:
    raise ValueError("Parallel must be a positive number or zero (all cpus). Got: " + str(parallel))

  if parallel == 1:
    return soloprocess_upload(QueueClass, queue_name, tasks)
    
  def capturing_soloprocess_upload(*args, **kwargs):
    try:
      return soloprocess_upload(*args, **kwargs)
    except Exception as err:
      print(err)
      error_queue.put(err)
    return 0

  uploadfn = partial(
    capturing_soloprocess_upload, QueueClass, queue_name
  )

  if isinstance(tasks, types.GeneratorType):
    try:
      task = next(item for item in tasks if item is not None)
    except StopIteration:
      return 0
    tasks = itertools.chain([task], tasks)

  # This is a hack to get dill to pickle dynamically
  # generated classes. This is an important use case
  # for when we create iterators with generator __iter__
  # functions on demand.

  # https://github.com/uqfoundation/dill/issues/56

  # cls_module = task.__class__.__module__
  # task.__class__.__module__ = '__main__'

  total = totalfn(tasks, total)

  block_size = 2000
  if total is not None and (total / parallel) < block_size:
    if total > 500:
      block_size = int(math.ceil(total / parallel))

  # Fix for MacOS which can segfault due to 
  # urllib calling libdispatch which is not fork-safe
  # https://bugs.python.org/issue30385
  no_proxy = os.environ.get("no_proxy", "")
  if platform.system().lower() == "darwin":
    os.environ["no_proxy"] = "*"

  ct = 0
  with tqdm(desc="Upload", total=total) as pbar:
    with pathos.pools.ProcessPool(parallel) as pool:
      for num_inserted in pool.imap(uploadfn, sip(tasks, block_size)):
        pbar.update(num_inserted)
        ct += num_inserted

  QueueClass(queue_name).add_insert_count(ct)

  if platform.system().lower() == "darwin":
    os.environ["no_proxy"] = no_proxy
  # task.__class__.__module__ = cls_module

  if not error_queue.empty():
    errors = []
    while not error_queue.empty():
      err = error_queue.get()
      if err is not StopIteration:
        errors.append(err)
    if len(errors):
      raise Exception(errors)

  return ct

# c/o https://stackoverflow.com/questions/12826291/raise-two-errors-at-the-same-time
def raise_multiple(errors):
  if not errors:
    return
  try:
    raise errors.pop()
  finally:
    raise_multiple(errors)


