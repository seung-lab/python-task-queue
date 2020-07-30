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
