import json
import os
import time

from six.moves import range
import pytest

import taskqueue
from taskqueue import RegisteredTask, TaskQueue, MockTask, PrintTask, LocalTaskQueue, MockTaskQueue
from taskqueue import QUEUE_NAME

TRAVIS_BRANCH = None if 'TRAVIS_BRANCH' not in os.environ else os.environ['TRAVIS_BRANCH']

if QUEUE_NAME != '':
  pass
elif TRAVIS_BRANCH is None:
  QUEUE_NAME = 'test-pull-queue'
elif TRAVIS_BRANCH == 'master':
  QUEUE_NAME = 'travis-pull-queue-1'
else:
  QUEUE_NAME = 'travis-pull-queue-2'


QTYPES = ('aws',)#, 'google')

QURL = 'https://sqs.us-east-1.amazonaws.com/098703261575/wms-test-pull-queue'

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

def test_get():
  global QUEUE_NAME

  for qtype in QTYPES:
    tq = TaskQueue(n_threads=0, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL)

    n_inserts = 5
    tq.purge()
    for _ in range(n_inserts):
      task = PrintTask()
      tq.insert(task)
    tq.wait()
    tq.purge()

def test_single_threaded_insertion():
  global QUEUE_NAME

  for qtype in QTYPES:
    tq = TaskQueue(n_threads=0, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL)

    tq.purge()

    if qtype != 'aws':
      assert tq.enqueued == 0
    
    n_inserts = 5
    for _ in range(n_inserts):
      task = PrintTask()
      tq.insert(task)
    tq.wait()

    if qtype != 'aws':
      assert tq.enqueued == n_inserts

    assert all(map(lambda x: type(x) == PrintTask, tq.list()))

    tq.purge()
    
    if qtype != 'aws':
      assert tq.enqueued == 0

def test_multi_threaded_insertion():
  global QUEUE_NAME
  for qtype in QTYPES:
    tq = TaskQueue(n_threads=40, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL)

    n_inserts = 100
    tq.purge()
    tq.wait()
    if qtype != 'aws':
      assert tq.enqueued == 0
    
    for _ in range(n_inserts):
      task = PrintTask()
      tq.insert(task)
    tq.wait()

    if qtype == 'aws':
      list_len = 10
    else:
      list_len = 100

    lst = tq.list()
    assert len(lst) == list_len # task list api only lists 100 items at a time
    assert all(map(lambda x: type(x) == PrintTask, lst))
    tq.purge()
    if qtype != 'aws':
      assert tq.enqueued == 0

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

def test_400_errors():
  global QUEUE_NAME
  for qtype in QTYPES:
    with TaskQueue(n_threads=1, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL) as tq:
      tq.delete('nonexistent')

def test_local_taskqueue():
  with LocalTaskQueue(parallel=True, progress=False) as tq:
    for i in range(20000):
      tq.insert(
        MockTask(arg=i)
      )

  with LocalTaskQueue(parallel=True, progress=False) as tq:
    for i in range(200):
      tq.insert(ExecutePrintTask(), [i], { 'wow2': 4 })

  with MockTaskQueue(parallel=True, progress=False) as tq:
    for i in range(200):
      tq.insert(ExecutePrintTask(), [i], { 'wow2': 4 })