import json
import os
import time

from six.moves import range
import pytest

import taskqueue
from taskqueue import RegisteredTask, GreenTaskQueue, TaskQueue, MockTask, PrintTask, LocalTaskQueue, MockTaskQueue
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

QURL = 'test-pull-queue'

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
    for QueueClass in (TaskQueue, GreenTaskQueue):
      tq = QueueClass(n_threads=0, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL)

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
    for QueueClass in (TaskQueue, GreenTaskQueue):
      tq = QueueClass(n_threads=0, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL)

      tq.purge()
      
      n_inserts = 5
      for _ in range(n_inserts):
        task = PrintTask()
        tq.insert(task)
      tq.wait()

      assert all(map(lambda x: type(x) == PrintTask, tq.list()))

      tq.purge()

def test_multi_threaded_insertion():
  global QUEUE_NAME
  for qtype in QTYPES:
    for QueueClass in (TaskQueue, GreenTaskQueue):
      tq = QueueClass(n_threads=0, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL)

      n_inserts = 10
      tq.purge()
      tq.wait()
     
      for _ in range(n_inserts):
        task = PrintTask()
        tq.insert(task)
      tq.wait()

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

def test_400_errors():
  global QUEUE_NAME
  for qtype in QTYPES:
    for QueueClass in (TaskQueue, GreenTaskQueue):
      with QueueClass(n_threads=0, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL) as tq:
        tq.delete('nonexistent')

def test_local_taskqueue():
  with LocalTaskQueue(parallel=True, progress=False) as tq:
    for i in range(20000):
      tq.insert(
        MockTask(arg=i)
      )

  with LocalTaskQueue(parallel=1, progress=False) as tq:
    for i in range(200):
      tq.insert(ExecutePrintTask(), [i], { 'wow2': 4 })

  with LocalTaskQueue(parallel=True, progress=False) as tq:
    for i in range(200):
      tq.insert(ExecutePrintTask(), [i], { 'wow2': 4 })

  with LocalTaskQueue(parallel=True, progress=False) as tq:
    epts = [ PrintTask(i) for i in range(200) ]
    tq.insert_all(epts)

  with MockTaskQueue(parallel=True, progress=False) as tq:
    for i in range(200):
      tq.insert(ExecutePrintTask(), [i], { 'wow2': 4 })

def test_parallel_insert_all():
  import pathos_issue

  global QURL
  tq = GreenTaskQueue(QURL)

  tasks = pathos_issue.crt_tasks(5, 20)
  tq.insert_all(tasks, parallel=2)

  tq.purge()