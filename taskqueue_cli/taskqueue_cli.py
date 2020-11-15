import os

import click

from taskqueue import TaskQueue, __version__
from taskqueue.lib import toabs
from taskqueue.paths import get_protocol

def normalize_path(queuepath):
  if not get_protocol(queuepath):
    return "fq://" + toabs(queuepath)
  return queuepath

@click.group()
@click.version_option(version=__version__)
def main():
  """
  CLI tool for managing python-task-queue queues.

  https://github.com/seung-lab/python-task-queue
  """
  pass

@main.command()
def license():
  """Prints the license for this library and cli tool."""
  path = os.path.join(os.path.dirname(__file__), 'LICENSE')
  with open(path, 'rt') as f:
    print(f.read())

@main.command()
@click.argument("queuepath")
def rezero(queuepath):
  """Reset collected statistics for queue."""
  TaskQueue(normalize_path(queuepath)).rezero()

@main.command()
@click.argument("queuepath")
def status(queuepath):
  """Print vital statistics for queue."""
  tq = TaskQueue(normalize_path(queuepath))
  ins = tq.inserted
  enq = tq.enqueued
  comp = tq.completed
  leased = tq.leased
  print(f"Inserted: {ins}")
  if ins > 0:
    print(f"Enqueued: {enq} ({enq / ins * 100:.1f}% left)")
    print(f"Completed: {comp} ({comp / ins * 100:.1f}%)")
  else:
    print(f"Enqueued: {enq} (--% left)")
    print(f"Completed: {comp} (--%)")

  if enq > 0:
    print(f"Leased: {leased} ({leased / enq * 100:.1f}% of queue)")
  else:
    print(f"Leased: {leased} (--%) of queue")

@main.command()
@click.argument("queuepath")
def release(queuepath):
  """Release all tasks from their leases."""
  TaskQueue(normalize_path(queuepath)).release_all()
