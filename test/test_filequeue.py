import json
import os
import time

from six.moves import range
import pytest

import taskqueue
from taskqueue import RegisteredTask, TaskQueue, MockTask, PrintTask, LocalTaskQueue
from taskqueue.paths import ExtractedPath, mkpath

FILE_QURL = 'fq:///tmp/removeme/taskqueue/fq'
N = 1000

def crtq():
  tq = TaskQueue(FILE_QURL)
  tq.purge()
  tq.rezero()

  tq.insert(( PrintTask(i) for i in range(N) ))
  return tq

def test_release_all():
  tq = crtq()
  for _ in range(tq.enqueued):
    task = tq.lease(seconds=3600)

  now = int(time.time())
  for fname in os.listdir(tq.api.queue_path):
    assert int(fname.split('--')[0]) > now

  tq.release_all()
  now = int(time.time())
  for fname in os.listdir(tq.api.queue_path):
    assert int(fname.split('--')[0]) <= now

  tq.purge()

def test_count_completions():
  tq = crtq()
  executed = tq.poll(stop_fn=lambda executed: N <= executed)
  assert tq.completed == 0
  tq = crtq()
  tq.poll(stop_fn=lambda executed: N <= executed, tally=True)
  assert tq.completed == N

  tq.purge()

def test_count_insertions():
  tq = crtq()
  assert tq.inserted == N
  tq.rezero()
  assert tq.inserted == 0

  tq.purge()

def test_count_leases():
  tq = crtq()
  assert tq.leased == 0
  tq.lease(seconds=10000)
  assert tq.leased == 1
  tq.lease(seconds=10000)
  tq.lease(seconds=10000)
  tq.lease(seconds=10000)
  assert tq.leased == 4
  tq.release_all()
  assert tq.leased == 0

  tq.purge()

def test_renew():
  tq = TaskQueue(FILE_QURL)
  tq.purge()

  tq.insert(PrintTask('hello'))

  ts = lambda fname: int(fname.split('--')[0])
  ident = lambda fname: fname.split('--')[1]

  filenames = os.listdir(tq.api.queue_path)
  assert len(filenames) == 1
  filename = filenames[0]

  assert ts(filename) == 0
  identity = ident(filename)

  now = time.time()
  tq.renew(filename, 1)

  filenames = os.listdir(tq.api.queue_path)
  assert len(filenames) == 1
  filename = filenames[0]

  assert ts(filename) >= int(time.time()) + 1
  assert ident(filename) == identity

def test_enumerating_tasks():
  tq = TaskQueue(FILE_QURL)
  tq.purge()

  for _ in range(10):
    tq.insert(PrintTask('hello'))
    tq.insert(PrintTask('world'))

  lst = list(tq.tasks())
  
  assert len(lst) == 20
  hello = 0
  world = 0
  for task in lst:
    hello += int(task.txt == "hello")
    world += int(task.txt == "world")

  assert hello == 10
  assert world == 10

def test_is_empty():
  tq = TaskQueue(FILE_QURL)
  tq.purge()

  assert tq.is_empty() == True

  tq.insert(PrintTask("hello"))

  assert tq.is_empty() == False

  task = tq.lease()
  tq.delete(task)

  assert tq.is_empty() == True




