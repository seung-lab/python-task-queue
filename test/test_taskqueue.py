import json
import os
import time

from six.moves import range
import pytest

import taskqueue
from taskqueue import RegisteredTask, TaskQueue, MockTask, PrintTask, LocalTaskQueue
from taskqueue.paths import ExtractedPath, mkpath

TRAVIS_BRANCH = None if 'TRAVIS_BRANCH' not in os.environ else os.environ['TRAVIS_BRANCH']

if TRAVIS_BRANCH is None:
  QURL = 'test-pull-queue'
elif TRAVIS_BRANCH == 'master':
  QURL = 'travis-pull-queue-1'
else:
  QURL = 'travis-pull-queue-2'

FILE_QURL = '/tmp/removeme/taskqueue/fq'

QURLS = {
  'sqs': QURL,
  'fq': FILE_QURL,
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
def test_get(protocol):
  path = getpath(protocol) 
  tq = TaskQueue(path, n_threads=0)

  n_inserts = 5
  tq.purge()
  tq.insert(( PrintTask() for _ in range(n_inserts) ))
  
  for i in range(n_inserts):
    t = tq.lease()
    tq.delete(t)

@pytest.mark.parametrize('protocol', PROTOCOL)
def test_single_threaded_insertion(protocol):
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
def test_multi_threaded_insertion(protocol, green, threads):
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
def test_400_errors(protocol):
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
def test_parallel_insert_all(protocol):
  import pathos_issue

  path = getpath(protocol) 
  tq = TaskQueue(path, green=True)

  tasks = pathos_issue.crt_tasks(5, 20)
  tq.insert(tasks, parallel=2)

  tq.purge()