import fcntl
import functools
import itertools
import json
import math
import operator
import os.path
import random
import re
import shutil
import uuid
import time

import tenacity

from .lib import mkdir, jsonify, toiter, STRING_TYPES, sip, toabs, first

retry = tenacity.retry(
  reraise=True,
  stop=tenacity.stop_after_attempt(4),
  wait=tenacity.wait_random_exponential(0.5, 60.0),
)

@retry
def read_file(path, mode='rt', lock=False, block=False):
  f = open(path, mode)
  if lock:
    f = read_lock_file(f)
  data = f.read()
  f.close()
  return data

@retry
def write_file(
  path, file, mode='wt',
  fsync=False, lock=False,
  block=False
):
  f = open(path, mode)
  if lock:
    f = write_lock_file(f, block=block)
  f.write(file)
  if fsync:
    f.flush() # from application buffers -> OS buffers
    os.fsync(f.fileno()) # OS buffers -> disk
  f.close()

# @retry
# def touch_file(path):
#   open(path, 'a').close()

@retry
def move_file(src_path, dest_path):
  os.rename(src_path, dest_path)

@retry
def write_lock_file(fd, block=False):
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
  mode = fcntl.LOCK_EX
  if not block:
    mode |= fcntl.LOCK_NB

  fcntl.lockf(fd.fileno(), mode)
  return fd

@retry
def read_lock_file(fd, block=False):
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
  mode = fcntl.LOCK_SH
  if not block:
    mode |= fcntl.LOCK_NB

  fcntl.lockf(fd.fileno(), mode)
  return fd

def unlock_file(fd):
  fcntl.lockf(fd.fileno(), fcntl.LOCK_UN)
  return fd

def idfn(task):
  if isinstance(task, STRING_TYPES):
    ident = task
  else:
    try:
      ident = task.id
    except AttributeError:
      ident = task['id']

  if "--" in ident:
    ident = ident.split("--")[1]
  return os.path.splitext(ident)[0] # removes .json if present

def get_timestamp(filename):
  filename = os.path.basename(filename)
  return int(filename.split("--")[0])

def set_timestamp(filename, timestamp):
  old_timestamp, rest = filename.split('--')
  return "{}--{}".format(timestamp, rest)

def nowfn():
  return int(time.time())

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
    path = toabs(path)
    self.path = path

    self.movement_path = mkdir(os.path.join(path, 'movement'))
    self.queue_path = mkdir(os.path.join(path, 'queue'))
    self.completions_path = os.path.join(path, 'completions')
    self.insertions_path = os.path.join(path, 'insertions')
    self.batch_size = 10

  @property
  def enqueued(self):
    return len(self)

  @property
  def inserted(self):
    try:
      return int(read_file(self.insertions_path))
    except FileNotFoundError:
      return 0

  @property
  def completed(self):
    try:
      return os.path.getsize(self.completions_path)
    except FileNotFoundError:
      return 0

  @property
  def leased(self):
    now = nowfn()
    ct = 0
    for file in os.scandir(self.queue_path):
      ct += int(get_timestamp(file.name) > now)
    return ct

  @retry
  def insert(self, tasks, delay_seconds=0):
    tasks = toiter(tasks)

    timestamp = 0 # immediately available, never assigned
    if delay_seconds > 0:
      timestamp = nowfn() + delay_seconds # unix timestamp

    total = 0
    for task in tasks:
      identifier = str(uuid.uuid4())
      filename = "{}--{}.json".format(timestamp, identifier)
      task['id'] = identifier
      write_file(
        os.path.join(self.queue_path, filename),
        jsonify(task)
      )
      write_file(
        os.path.join(self.movement_path, identifier),
        filename + "\n"
      )
      total += 1

    return total

  def add_insert_count(self, ct):
    try:
      N = read_file(self.insertions_path) # potential multiprocess race condition
      N = int(N) if N != '' else 0
    except FileNotFoundError:
      N = 0

    N += int(ct)
    write_file(self.insertions_path, str(N), fsync=True, lock=True, block=True)
    return N

  @retry
  def rezero(self):
    # no sense acquiring a lock for completions since other writers aren't
    write_file(self.completions_path, b'', mode='bw+', fsync=True)
    write_file(self.insertions_path, '0', mode='tw+', fsync=True, lock=True, block=True)

  @retry
  def renew_lease(self, task, seconds):
    ident = idfn(task)
    movement_path = os.path.join(self.movement_path, ident)

    fd = read_lock_file(open(movement_path, 'rt'))
    contents = fd.read()
    fd.close()

    fd = write_lock_file(open(movement_path, 'wt'))

    for filename in reversed(contents.split('\n')):
      if filename == '':
        continue

      old_path = os.path.join(self.queue_path, filename)
      new_filename = set_timestamp(filename, nowfn() + int(seconds))
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
    self.renew_lease(task, 0)

  def release_all(self):
    """Voids all leases and sets all tasks to available."""
    now = nowfn()
    for file in os.scandir(self.queue_path):
      if get_timestamp(file.name) < now:
        continue

      new_filename = set_timestamp(file.name, now)
      move_file(
        os.path.join(self.queue_path, file.name),
        os.path.join(self.queue_path, new_filename)
      )

      movement_path = os.path.join(self.movement_path, idfn(new_filename))
      fd = write_lock_file(open(movement_path, 'at'))
      fd.write(new_filename + "\n")
      fd.close()

  @retry
  def _lease_filename(self, filename, seconds):
    new_filename = set_timestamp(filename, nowfn() + int(seconds))
    new_filepath = os.path.join(self.queue_path, new_filename)
    movements_filename = idfn(new_filename)
    movements_path = os.path.join(self.movement_path, movements_filename)

    fd = write_lock_file(open(movements_path, 'at'))

    try:
      move_file(
        os.path.join(self.queue_path, filename),
        new_filepath
      )
    except FileNotFoundError:
      fd.close()
      return None

    fd.write(os.path.basename(str(new_filepath)) + '\n')

    fd.flush()
    fd.close() # unlocks POSIX advisory file lock

    return json.loads(read_file(new_filepath))

  def lease(self, seconds, num_tasks, wait_sec=None):
    if wait_sec is None:
      wait_sec = 0

    def fmt(direntry):
      filename = direntry.name
      timestamp, _ = filename.split('--')
      return (int(timestamp), filename)

    now = nowfn()
    files = ( fmt(direntry) for direntry in os.scandir(self.queue_path) )

    leasable_files = []

    for batch in sip(files, 250):
      random.shuffle(batch)

      for timestamp, filename in batch:
        if timestamp > now:
          continue
        leasable_files.append(filename)
        if len(leasable_files) >= num_tasks:
          break

      if len(leasable_files) >= num_tasks:
        break

    leases = []
    for filename in leasable_files:
      try:
        lessee = self._lease_filename(filename, seconds)
      except OSError:
        continue

      if lessee is not None:
        leases.append(lessee)

    wait_leases = []
    if wait_sec > 0 and len(leases) < num_tasks:
      # Add a constant b/c this will cascade into shorter and 
      # shorter checks as wait_sec shrinks and we don't
      # want hundreds of workers to accidently synchronize
      sleep_amt = random.random() * (wait_sec + 1)

      # but we still want to guarantee that wait_sec is not
      # exceeded.
      sleep_amt = min(sleep_amt, wait_sec)
      time.sleep(sleep_amt)
      wait_leases = self.lease(
        seconds, 
        num_tasks - len(leases), 
        wait_sec - sleep_amt
      )

    return leases + wait_leases

  @retry
  def delete(self, task):
    ident = idfn(task)

    movements_file_path = os.path.join(self.movement_path, ident)
    try:
      fd = read_lock_file(open(movements_file_path, 'rt'))
    except FileNotFoundError:
      # if it doesn't exist we still want to count this
      # as a delete request succeeded b/c its purpose was
      # achieved and the progress bar should increment.
      return 1

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
    try:
      os.remove(movements_file_path)
    except FileNotFoundError:
      pass

    return 1

  def tally(self):
    with open(self.completions_path, 'ba') as f:
      f.write(b'\0')

  def purge(self, native=False):
    # native only has meaning for SQS for now
    # but we have to accept it as a parameter.
    all_files = itertools.chain(
      os.scandir(self.queue_path),
      os.scandir(self.movement_path)
    )
    for file in all_files:
      try:
        os.remove(file.path)
      except FileNotFoundError:
        pass

    self.rezero()

  def is_empty(self):
    try:
      first(os.scandir(self.queue_path))
      return False
    except StopIteration:
      return True

  def __iter__(self):
    def read(path):
      with open(path, 'rt') as f:
        return f.read()

    for f in os.scandir(self.queue_path):
      try:
        yield read(f.path)
      # It's possible for a task to have been
      # deleted in between scanning and reading.
      except FileNotFoundError:
        continue

  def __len__(self):
    return functools.reduce(operator.add, ( 1 for f in os.scandir(self.queue_path) ), 0)







