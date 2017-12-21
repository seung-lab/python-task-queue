import os

from builtins import range
import pytest

from taskqueue import RegisteredTask, TaskQueue
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

class MockTask(RegisteredTask):
  def __init__(self):
    super(MockTask, self).__init__()

def test_get():
  global QUEUE_NAME
  tq = TaskQueue(n_threads=0, queue_name=QUEUE_NAME, queue_server='pull-queue')

  n_inserts = 5
  tq.purge()
  for _ in range(n_inserts):
    task = MockTask()
    tq.insert(task)
  tq.wait()
  tq.purge()

def test_single_threaded_insertion():
  global QUEUE_NAME
  tq = TaskQueue(n_threads=0, queue_name=QUEUE_NAME, queue_server='pull-queue').purge()
  
  n_inserts = 5
  for _ in range(n_inserts):
    task = MockTask()
    tq.insert(task)

  lst = tq.list()
  assert 'items' in lst
  items = lst['items']
  assert len(items) == n_inserts

  tags = map(lambda x: x['tag'], items)
  assert all(map(lambda x: x == MockTask.__name__, tags))

  tq.purge()
  assert not 'items' in tq.list()
  assert tq.enqueued == 0


def test_multi_threaded_insertion():
  global QUEUE_NAME
  tq = TaskQueue(n_threads=40, queue_name=QUEUE_NAME, queue_server='pull-queue')

  n_inserts = 100
  tq.purge()
  tq.wait()
  assert tq.enqueued == 0
  
  for _ in range(n_inserts):
    task = MockTask()
    tq.insert(task)
  tq.wait()

  lst = tq.list()
  assert 'items' in lst
  items = lst['items']
  assert len(items) == 100 # task list api only lists 100 items at a time
  tags = map(lambda x: x['tag'], items)
  assert all(map(lambda x: x == MockTask.__name__, tags))
  tq.purge()
  assert tq.enqueued == 0

def test_400_errors():
  global QUEUE_NAME
  with TaskQueue(n_threads=1, queue_name=QUEUE_NAME) as tq:
    tq.delete('nonexistent')




