import fcntl
import itertools
import json
import os.path
import re
import shutil
import uuid
import time

import tenacity

from .lib import mkdir, jsonify, toiter, STRING_TYPES, sip

retry = tenacity.retry(
  reraise=True, 
  stop=tenacity.stop_after_attempt(7), 
  wait=tenacity.wait_random_exponential(0.5, 60.0),
)

@retry
def read_file(path, mode='rt'):
  with open(path, mode) as f:
    return f.read()

@retry
def write_file(path, file, mode='wt'):
  with open(path, mode) as f:
    f.write(file)

@retry
def touch_file(path):
  open(path, 'a').close()

@retry
def move_file(src_path, dest_path):
  os.rename(src_path, dest_path)

def write_lock_file(fd):
  """
  Locks are bound to processes. A terminated process unlocks. 
  Non-blocking, raises OSError if unable to obtain a lock.

  Note that any closing of a file descriptor for the locked file
  will release locks for all fds. This means you must open the file
  and reuse that FD from start to finish.
  """

  # https://docs.python.org/3/library/fcntl.html
  # "On at least some systems, LOCK_EX can only be used if the file 
  # descriptor refers to a file opened for writing."
  # Locks: LOCK_EX (exclusive), LOCK_SH (shared), LOCK_NB (non-blocking)

  fcntl.lockf(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  return fd

def read_lock_file(fd):
  """
  Locks are bound to processes. A terminated process unlocks. 
  Non-blocking, raises OSError if unable to obtain a lock.

  Note that any closing of a file descriptor for the locked file
  will release locks for all fds. This means you must open the file
  and reuse that FD from start to finish.
  """

  # https://docs.python.org/3/library/fcntl.html
  # "On at least some systems, LOCK_EX can only be used if the file 
  # descriptor refers to a file opened for writing."
  # Locks: LOCK_EX (exclusive), LOCK_SH (shared), LOCK_NB (non-blocking)

  fcntl.lockf(fd.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
  return fd

def unlock_file(fd):
  fcntl.lockf(fd, fcntl.LOCK_UN)
  return fd

def get_id(task):
  if isinstance(task, STRING_TYPES):
    ident = task
  else:
    try:
      ident = task._id
    except AttributeError:
      ident = task['id']
  return ident

def set_timestamp(filename, timestamp):
  old_timestamp, rest = filename.split('--')
  return "{}--{}".format(timestamp, rest)

def add_seconds_to_timestamp(filename, seconds):
  timestamp, rest = filename.split('--')
  timestamp = int(timestamp) + int(seconds)
  return "{}--{}".format(timestamp, rest)

class FileQueueAPI(object):
  """
  University clusters and supercomputers often cannot access SQS easily 
  but have access to a common file system. It would be a pain to have t
  o set up a RabbitMQ instance or similar process on each cluster we 
  get access to, so it would be ideal to have a queue system that just 
  runs off the filesystem.

  We need the following properties from our queue:

    Serverless
    Durable - No losses from power outages and process crashes.
    Supports Verbs - queue create, queue delete, task create, 
      time limited task lease, task delete, task lease extend, 
      and reset tasks leases.
    Parallel Safe
    Recirculating Tasks - If a process fails, eventually the 
      task will be picked up by another one.
    Supports millions of tasks.
    Can be operated by a pipeline technician without help 
      (or need onerous approvals) from a cluster administrator.

  File Queues in principle fulfill the first two properties as the 
  server is the filesystem and files do not disappear on power loss 
  or process crash. On journaling filesystems, the files do not even 
  become corrupted on power loss in the middle of writing. Filesystems 
  support millions of files in a single directory, but certain operations 
  like listing become unusable. Properties 3 through 6 will require 
  careful design. We anticipate that these queues can be run from 
  userland and require no special approvals to be used unless the queues 
  are very large, in which case the entire job will likely need special 
  approval anyway.

  With respect to the verbs specified, all should be familiar from SQS 
  with one exception: reset task leases is new and is extremely useful 
  for resetting a job that has partially run but crashed when the lease 
  time is very long.
  """
  def __init__(self, path):
    self.path = path

    self.movement_path = mkdir(os.path.join(path, 'movement'))
    self.queue_path = mkdir(os.path.join(path, 'queue'))

  @property
  def enqueued(self):
    return int(self.list())

  def status(self):
    resp = self._sqs.get_queue_attributes(
      QueueUrl=self._qurl, 
      AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    return resp['Attributes']

  def insert(self, tasks, delay_seconds=0):
    tasks = toiter(tasks)

    timestamp = 0 # immediately available, never assigned
    if delay_seconds > 0:
      timestamp = int(time.time()) + delay_seconds # unix timestamp

    for task in tasks:
      identifier = uuid.uuid4()
      filename = "{}--{}.json".format(timestamp, identifier)
      write_file(
        os.path.join(self.queue_path, filename),
        jsonify(task)
      )
      touch_file(
        os.path.join(self.movement_path, str(identifier))
      )

  def renew_lease(self, task, seconds):
    ident = get_id(task)
    movement_path = os.path.join(self.movement_path, ident)

    fd = read_lock_file(open(movements_path, 'rt'))
    contents = fd.read()

    for filename in reversed(contents.split('\n')):
      if filename == '':
        continue

      old_path = os.path.join(self.queue_path, filename)
      new_filename = add_seconds_to_timestamp(filename, seconds)
      new_path = os.path.join(self.queue_path, new_filename)
      try:
        move_file(old_path, new_path)
      except FileNotFoundError:
        continue

      try:
        fd.write(contents + new_filename + '\n')
      except:
        move_file(new_path, old_path)
        fd.close()
        raise

      break

    fd.close() # releases POSIX lock

  def cancel_lease(self, task):
    raise NotImplementedError()

  def make_all_available(self):
    """Voids leases and sets all tasks to available."""
    now = int(time.time())
    for file in os.scandir(self.movement_path):
      try:
        os.remove(file.path, file.name)
      except FileNotFoundError:
        pass   

    for file in os.scandir(self.queue_path):
      move_file(
        os.path.join(self.queue_path, file.name),
        os.path.join(self.queue_path, set_timestamp(file.name, now))
      )

  def _lease_filename(self, filename, seconds):
    timestamp, rest = filename.split('--')
    now = int(time.time())
    new_filename = "{}--{}".format(now + seconds, rest)
    new_filepath = os.path.join(self.queue_path, new_filename)

    movements_filename = rest.split('.')[0] # uuid
    movements_path = os.path.join(self.movement_path, movements_filename)

    fd = open(movements_path, 'at')
    write_lock_file(fd)

    move_file(
      os.path.join(self.queue_path, filename), 
      new_filepath
    )

    fd.write(str(filename) + '\n')

    fd.flush()
    fd.close() # unlocks POSIX advisory file lock

    task = json.loads(read_file(new_filepath))
    task['id'] = movements_filename
    return task

  def lease(self, seconds, num_tasks):
    def fmt(direntry):
      filename = direntry.name
      timestamp, _ = filename.split('--')
      return (int(timestamp), filename)

    now = int(time.time())
    files = ( fmt(direntry) for direntry in os.scandir(self.queue_path) )
    
    for timestamp, filename in files:
      if timestamp > now:
        continue
      else:
        try:
          return [ self._lease_filename(filename, seconds) ]
        except OSError:
          continue

    return []

  def acknowledge(self, task):
    return self.delete(task)

  def delete(self, task):
    ident = get_id(task)

    movements_file_path = os.path.join(self.movement_path, ident)
    fd = read_lock_file(open(movements_file_path, 'rt'))
    filenames = fd.read().split('\n')
    fd.close()

    fd = write_lock_file(open(movements_file_path, 'wt'))    

    for filename in filenames:
      if filename == '':
        continue

      try:
        os.remove(os.path.join(self.queue_path, filename))
      except FileNotFoundError:
        pass

    fd.close()
    os.remove(movements_file_path)

  def purge(self):
    all_files = itertools.chain(
      os.scandir(self.queue_path), 
      os.scandir(self.movement_path)
    )
    for file in all_files:
      try:
        os.remove(file.path)
      except FileNotFoundError:
        pass

  def is_empty(self):
    try:
      first(iter(self))
      return False
    except StopIteration:
      return True

  def __iter__(self):
    return ( f.name for f in os.scandir(self.queue_path) )

  def __len__(self):
    return itertools.count(iter(self))
      
      





