from functools import partial
import json
import os
import time

from moto import mock_sqs

from six.moves import range
import pytest

import taskqueue
from taskqueue import (
  queueable, FunctionTask, RegisteredTask, 
  TaskQueue, MockTask, PrintTask, LocalTaskQueue,
  QueueEmptyError
)
from taskqueue.paths import ExtractedPath, mkpath
from taskqueue.queueables import totask
from taskqueue.queueablefns import tofunc, UnregisteredFunctionError, func2task

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

@queueable
def printfn(txt):
  print(txt)
  return 1337

@queueable
def sumfn(a,b):
  return a + b

def test_task_creation_fns():
  task = partial(printfn, "hello world")
  task = totask(task)

  assert task.key == ("test_taskqueue", "printfn")
  assert task.args == ["hello world"]
  assert task.kwargs == {}
  assert task.id == -1

  fn = tofunc(task)
  assert fn() == 1337

  fn = partial(partial(sumfn, 1), 2)
  assert func2task(fn, -1)() == 3

  fn = partial(partial(sumfn, 1), b=2)
  assert func2task(fn, -1)() == 3

  task = partial(printfn, not_a_real_arg="hello world")
  try:
    task = totask(task)
    assert False
  except TypeError:
    pass

  try:
    task = totask(printfn) # not enough args
    assert False
  except TypeError:
    pass

  task = partial(printfn, "hello world", "omg")
  try:
    task = totask(task)
    assert False
  except TypeError:
    pass

  try:
    FunctionTask(("fake", "fake"), [], {}, None)()
    assert False, "Should not have been able to call this function."
  except UnregisteredFunctionError:
    pass

def test_task_creation_classes():
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

def test_queue_transfer(sqs):
  tqsqs = TaskQueue(getpath("sqs"))
  tqsqs.purge()
  tqfq = TaskQueue(getpath("fq"))
  tqfq.purge()

  assert tqsqs.enqueued == 0

  tqfq.insert(( PrintTask() for _ in range(10) ))
  tqsqs.insert(tqfq)

  assert tqsqs.enqueued == 10
  task = tqsqs.lease()
  assert isinstance(task, PrintTask)

  try:
    tqfq.insert(tqsqs)
    assert False
  except taskqueue.UnsupportedProtocolError:
    pass

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

def test_lease(sqs):
  path = getpath("sqs") 
  tq = TaskQueue(path, n_threads=0)

  n_inserts = 20
  tq.purge()
  tq.insert(( PrintTask(str(x)) for x in range(n_inserts) ))

  tasks = tq.lease(num_tasks=10, wait_sec=0)
  assert len(tasks) == 10
  tq.delete(tasks)

  tasks = tq.lease(num_tasks=10, wait_sec=0)
  assert len(tasks) == 10
  tq.delete(tasks)

  try:
    tasks = tq.lease(num_tasks=10, wait_sec=0)
    assert False
  except QueueEmptyError:
    pass

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
def test_single_threaded_insertion_fns(sqs, protocol):
  path = getpath(protocol) 
  tq = TaskQueue(path, n_threads=0)

  tq.purge()
  
  n_inserts = 5
  tq.insert(( partial(printfn, "hello world " + str(i)) for i in range(n_inserts) ))

  assert all(map(lambda x: isinstance(x, FunctionTask), tq.list()))

  tq.purge()

@pytest.mark.parametrize('protocol', PROTOCOL)
@pytest.mark.parametrize('green', (True, False))
@pytest.mark.parametrize('threads', (1, 2, 10, 20, 40))
def test_multi_threaded_insertion(sqs, protocol, green, threads):
  path = getpath(protocol) 

  tq = TaskQueue(path, n_threads=threads, green=green)

  n_inserts = 40
  tq.purge()
  ct = tq.insert(( PrintTask() for i in range(n_inserts)))
  tq.purge()

  assert ct == n_inserts

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
  assert tq.insert(tasks) == 20000

  tq = LocalTaskQueue(parallel=1, progress=False)
  tasks = ( (ExecutePrintTask(), [i], { 'wow2': 4 }) for i in range(200) )
  assert tq.insert(tasks) == 200

  tq = LocalTaskQueue(parallel=True, progress=False)
  tasks = ( (ExecutePrintTask(), [i], { 'wow2': 4 }) for i in range(200) )
  assert tq.insert(tasks) == 200

  tq = LocalTaskQueue(parallel=True, progress=False)
  epts = [ PrintTask(i) for i in range(200) ]
  assert tq.insert(epts) == 200

@pytest.mark.parametrize('protocol', PROTOCOL)
def test_parallel_insert_all(sqs, protocol):
  import pathos_issue

  path = getpath(protocol) 
  tq = TaskQueue(path, green=True)
  tq.purge()

  if protocol == 'fq':
    tq.rezero()

  tasks = pathos_issue.crt_tasks(5, 20)
  amt = tq.insert(tasks, parallel=2)

  assert amt == 15 
  if protocol == 'fq':
    assert tq.inserted == 15

  tq.purge()

def test_polling(sqs):
  N = 100
  tasks = [ PrintTask(i) for i in range(N) ]
  tq = TaskQueue(getpath('fq'), green=False)
  tq.purge()
  tq.insert(tasks)

  tq.poll(
    lease_seconds=1, 
    verbose=False, 
    tally=True, 
    stop_fn=(lambda executed: executed >= 5)
  )

  tq.purge()
  tq.insert(tasks)

  tq.poll(
    lease_seconds=1, 
    verbose=False, 
    tally=True, 
    stop_fn=(lambda elapsed_time: elapsed_time >= 1)
  )


