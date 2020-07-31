import json
import os
import time

from moto import mock_sqs

from six.moves import range
import pytest

import taskqueue
from taskqueue import RegisteredTask, TaskQueue, MockTask, PrintTask, LocalTaskQueue
from taskqueue.paths import ExtractedPath, mkpath


@pytest.fixture(scope='function')
def aws_credentials():
  """Mocked AWS Credentials for moto."""
  os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
  os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
  os.environ['AWS_SECURITY_TOKEN'] = 'testing'
  os.environ['AWS_SESSION_TOKEN'] = 'testing'
  os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def sqs(aws_credentials):
  with mock_sqs():
    import boto3
    client = boto3.client('sqs')
    client.create_queue(QueueName='test-pull-queue')
    yield client

QURLS = {
  'sqs': 'test-pull-queue',
  'fq': '/tmp/removeme/taskqueue/fq',
}

PROTOCOL = ('fq', 'sqs')

def getpath(protocol):
  global QURLS
  qurl = QURLS[protocol]
  return mkpath(ExtractedPath(protocol, qurl))

class ExecutePrintTask(RegisteredTask):
  def __init__(self):
    super(ExecutePrintTask, self).__init__()

  def execute(self, wow, wow2):
    print(wow + wow2)
    return wow + wow2

def test_task_creation():
  task = MockTask(this="is", a=[1, 4, 2], simple={"test": "to", "check": 4},
               serialization=('i', 's', 's', 'u', 'e', 's'), wow=4, with_kwargs=None)
  task.wow = 5
  payload = task.payload()
  payload = json.dumps(payload)
  task_deserialized = MockTask.deserialize(payload)

  assert task_deserialized._args == {
      "this": "is", "a": [1, 4, 2],
      "simple": {"test": "to", "check": 4},
      "serialization": ["i", "s", "s", "u", "e", "s"],
      "wow": 5,
      "with_kwargs": None
  }

@pytest.mark.parametrize('protocol', PROTOCOL)
def test_get(sqs, protocol):
  path = getpath(protocol) 
  tq = TaskQueue(path, n_threads=0)

  n_inserts = 5
  tq.purge()
  tq.insert(( PrintTask() for _ in range(n_inserts) ))
  
  for i in range(n_inserts):
    t = tq.lease()
    tq.delete(t)

@pytest.mark.parametrize('protocol', PROTOCOL)
def test_single_threaded_insertion(sqs, protocol):
  path = getpath(protocol) 
  tq = TaskQueue(path, n_threads=0)

  tq.purge()
  
  n_inserts = 5
  tq.insert(( PrintTask() for i in range(n_inserts) ))

  assert all(map(lambda x: type(x) == PrintTask, tq.list()))

  tq.purge()

@pytest.mark.parametrize('protocol', PROTOCOL)
@pytest.mark.parametrize('green', (True, False))
@pytest.mark.parametrize('threads', (1, 2, 10, 20, 40))
def test_multi_threaded_insertion(sqs, protocol, green, threads):
  path = getpath(protocol) 

  tq = TaskQueue(path, n_threads=threads, green=green)

  n_inserts = 40
  tq.purge()
  tq.insert(( PrintTask() for i in range(n_inserts)))
  tq.purge()

# def test_multiprocess_upload():
#   global QURL

#   with TaskQueue(QURL) as tq:
#     tq.purge()

#   time.sleep(1)

#   num_tasks = 1000
#   tasks = [ PrintTask(i) for i in range(num_tasks) ]

#   taskqueue.upload(QURL, tasks, parallel=4)

#   time.sleep(1)
#   try:
#     assert tq.enqueued == num_tasks
#   finally:
#     with TaskQueue(QURL) as tq:
#       tq.purge()


@pytest.mark.parametrize('protocol', PROTOCOL)
def test_400_errors(sqs, protocol):
  path = getpath(protocol) 

  tq = TaskQueue(path, n_threads=0)
  tq.delete('nonexistent')

def test_local_taskqueue():
  tq = LocalTaskQueue(parallel=True, progress=False)
  tasks = ( MockTask(arg=i) for i in range(20000) )
  tq.insert(tasks)

  tq = LocalTaskQueue(parallel=1, progress=False)
  tasks = ( (ExecutePrintTask(), [i], { 'wow2': 4 }) for i in range(200) )
  tq.insert(tasks)

  tq = LocalTaskQueue(parallel=True, progress=False)
  tasks = ( (ExecutePrintTask(), [i], { 'wow2': 4 }) for i in range(200) )
  tq.insert(tasks)

  tq = LocalTaskQueue(parallel=True, progress=False)
  epts = [ PrintTask(i) for i in range(200) ]
  tq.insert(epts)

@pytest.mark.parametrize('protocol', PROTOCOL)
def test_parallel_insert_all(sqs, protocol):
  import pathos_issue

  path = getpath(protocol) 
  tq = TaskQueue(path, green=True)

  tasks = pathos_issue.crt_tasks(5, 20)
  tq.insert(tasks, parallel=2)

  tq.purge()