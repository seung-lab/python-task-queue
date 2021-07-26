import os
import math

import click
from tqdm import tqdm

from taskqueue import TaskQueue, __version__, QueueEmptyError
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

  if not math.isnan(ins):
    print(f"Inserted: {ins}")

  if ins > 0:
    print(f"Enqueued: {enq} ({enq / ins * 100:.1f}% left)")
    if not math.isnan(comp):
      print(f"Completed: {comp} ({comp / ins * 100:.1f}%)")
  else:
    print(f"Enqueued: {enq} (--% left)")
    if not math.isnan(comp):
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

@main.command()
@click.argument("src")
@click.argument("dest")
def cp(src, dest):
  """
  Copy the contents of a queue to another
  service or location. Do not run this
  process while a queue is being worked.

  Currently sqs queues are not copiable,
  but you can copy an fq to sqs. The mv
  command supports sqs queues.
  """
  src = normalize_path(src)
  dest = normalize_path(dest)

  if get_protocol(src) == "sqs":
    print("ptq: cp does not support sqs:// as a source.")
    return

  tqd = TaskQueue(dest)
  tqs = TaskQueue(src)

  tqd.insert(tqs)

@main.command()
@click.argument("src")
@click.argument("dest")
def mv(src, dest):
  """
  Moves the contents of a queue to another
  service or location. Do not run this
  process while a queue is being worked.

  Moving an sqs queue to a file queue
  may result in duplicated tasks.
  """
  src = normalize_path(src)
  dest = normalize_path(dest)

  tqd = TaskQueue(dest, progress=False)
  tqs = TaskQueue(src, progress=False)

  total = tqs.enqueued
  with tqdm(total=total, desc="Moving") as pbar:
    while True:
      try:
        tasks = tqs.lease(num_tasks=10, seconds=10)
      except QueueEmptyError:
        break

      tqd.insert(tasks)
      tqs.delete(tasks)
      pbar.update(len(tasks))

@main.command()
@click.argument("queuepath")
def purge(queuepath):
  """Delete all queued messages and zero out queue statistics."""
  queuepath = normalize_path(queuepath)
  tq = TaskQueue(queuepath)
  tq.purge()




