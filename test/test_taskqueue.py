import os
import time

from six.moves import range
import pytest

from taskqueue import RegisteredTask, TaskQueue, MockTask, PrintTask, LocalTaskQueue
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


QTYPES = ('appengine', 'aws')#, 'google')

QURL = 'https://sqs.us-east-1.amazonaws.com/098703261575/wms-test-pull-queue'

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

def test_400_errors():
  global QUEUE_NAME
  for qtype in QTYPES:
    with TaskQueue(n_threads=1, queue_name=QUEUE_NAME, queue_server=qtype, qurl=QURL) as tq:
      tq.delete('nonexistent')

def test_local_taskqueue():
  with LocalTaskQueue(parallel=True, progress=False) as tq:
    for i in range(20000):
      tq.insert(
        MockTask(i)
      )