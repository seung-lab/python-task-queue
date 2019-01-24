[![Build Status](https://travis-ci.org/seung-lab/python-task-queue.svg?branch=master)](https://travis-ci.org/seung-lab/python-task-queue) [![PyPI version](https://badge.fury.io/py/task-queue.svg)](https://badge.fury.io/py/task-queue) 

# python-task-queue
Python TaskQueue object that can rapidly populate and download from cloud queues. Supports local multi-process execution as well.

# Installation

```bash
pip install numpy # make sure you do this first on a seperate line
pip install task-queue
```

The task queue uses your CloudVolume secrets located in `$HOME/.cloudvolume/secrets/`. When using AWS SQS as your queue backend, you must provide `$HOME/.cloudvolume/secrets/aws-secret.json`. See the [CloudVolume](https://github.com/seung-lab/cloud-volume) repo for additional instructions.  

The additional pip install line is to make it easier for CloudVolume to install as this library uses its facilities for accessing secrets.

# Usage 

Define a class that inherits from taskqueue.RegisteredTask and implments the `execute` method.

Tasks can be loaded into queues locally or as based64 encoded data in the cloud and executed later.
Here's an example implementation of a `PrintTask`. Generally, you should specify a very lightweight
container and let the actual execution download and manipulate data.

```python
from taskqueue import RegisteredTask

class PrintTask(RegisteredTask):
  def __init__(self, txt=''):
    super(PrintTask, self).__init__(txt)
    self.txt = txt

  def execute(self):
    if self.txt:
      print(str(self) + ": " + str(self.txt))
    else:
      print(self)
```

## Local Usage

For small jobs, you might want to use one or more processes to execute the tasks:
```python
from taskqueue import LocalTaskQueue

with LocalTaskQueue(parallel=5) as tq: # use 5 processes
  for _ in range(1000):
    tq.insert(
      PrintTask(i)
    )
```
This will load the queue with 1000 print tasks then execute them across five processes.

## Cloud Usage

Set up an SQS queue and acquire an aws-secret.json that is compatible with CloudVolume.

```python
from taskqueue import TaskQueue

qurl = 'https://sqs.us-east-1.amazonaws.com/$DIGITS/$QUEUE_NAME'
with TaskQueue(queue_server='sqs', qurl=qurl) as tq:
  for _ in range(1000):
    tq.insert(PrintTask(i))
```

This inserts 1000 PrintTask descriptions into your SQS queue.

Somewhere else, you'll do the following (probably across multiple workers):

```python
from taskqueue import TaskQueue

qurl = 'https://sqs.us-east-1.amazonaws.com/$DIGITS/$QUEUE_NAME'
with TaskQueue(queue_server='sqs', qurl=qurl) as tq:
  task = tq.lease(seconds=int($LEASE_SECONDS))
  task.execute()
  tq.delete(task)
```