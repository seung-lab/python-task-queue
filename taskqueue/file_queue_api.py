import fcntl
import json
import os.path
import re
import shutil
import uuid
import time

import tenacity

from .lib import mkdir, jsonify, toiter, STRING_TYPES

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

class FileQueue(object):
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

    if os.path.exists(path):
      raise ValueError("This queue or a similarly named directory already exists: " + str(path))

    self.movement_path = mkdir(os.path.join(path, 'movement'))
    self.queue_path = mkdir(os.path.join(path, 'queue'))

  @property
  def enqueued(self):
    status = self.status()
    return int(status['ApproximateNumberOfMessages']) + int(status['ApproximateNumberOfMessagesNotVisible'])

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

  def renew_lease(self, seconds):
    raise NotImplementedError() 

  def cancel_lease(self, rhandle):
    raise NotImplementedError()

  def _lease_filename(self, filename):
    timestamp, rest = filename.split('--')
    now = int(time.time())
    new_filename = "{}--{}".format(now + seconds, rest)
    new_filepath = os.path.join(self.queue_path, new_filename)

    move_file(
      os.path.join(self.queue_path, filename), 
      new_filepath
    )

    movements_filename = rest.split('.')[0] # uuid
    movement_path = os.path.join(self.movement_path, movements_filename)
    write_file(movements_path, str(filename) + '\n', mode='at')

    task = json.loads(read_file(new_filepath))
    task['id'] = movements_filename
    return task

  @retry
  def lease(self, seconds):
    def fmt(direntry):
      filename = direntry.name
      timestamp, _ = filename.split('--')
      return (int(timestamp), filename)

    now = int(time.time())
    files = ( fmt(direntry) for direntry in os.scandir(self.queue_path) )
    
    for timestamp, filename in files:
      if timestamp > now:
        continue
      if timestamp < mintime:
        try:
          return [ self._lease_filename(filename) ]
        except OSError:
          continue

    return []

  def acknowledge(self, task):
    return self.delete(task)

  def delete(self, task):
    if isinstance(task, STRING_TYPES):
      ident = task
    else:
      try:
        ident = task._id
      except AttributeError:
        ident = task['id']

    movements_filename = os.path.join(self.movement_path, ident)
    filenames = read_file(movements_filename).split('\n')

    for filename in filenames:
      if filename == '':
        continue

      try:
        os.remove(os.path.join(self.queue_path, filename))
      except FileNotFoundError:
        pass

    os.remove(movements_filename)

  def purge(self):
    # This is more efficient, but it kept freezing
    # try:
    #     self._sqs.purge_queue(QueueUrl=self._qurl)
    # except botocore.errorfactory.PurgeQueueInProgress:

    while self.enqueued:
      # visibility_timeout must be > 0 for delete to work
      tasks = self._request(num_tasks=10, visibility_timeout=10)
      for task in tasks:
        self.delete(task)
    return self

  def list(self):
    return ( f.name for f in os.scandir(self.movement_path) )
      
      





