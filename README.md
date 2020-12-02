[![Build Status](https://travis-ci.org/seung-lab/python-task-queue.svg?branch=master)](https://travis-ci.org/seung-lab/python-task-queue) [![PyPI version](https://badge.fury.io/py/task-queue.svg)](https://badge.fury.io/py/task-queue)

# python-task-queue

This package provides a client and system for generating, uploading, leasing, and executing dependency free tasks both locally and in the cloud using AWS SQS or on a single machine or cluster with a common file system using file based queues.

## Installation

```bash
pip install numpy # make sure you do this first on a seperate line
pip install task-queue
```

The task queue uses your CloudVolume secrets located in `$HOME/.cloudvolume/secrets/`. When using AWS SQS as your queue backend, you must provide `$HOME/.cloudvolume/secrets/aws-secret.json`. See the [CloudVolume](https://github.com/seung-lab/cloud-volume) repo for additional instructions.

## Usage

As of version 2.7.0, there are two ways to create a queueable task. The new way is simpler and probably preferred.

### New School: Queuable Functions

Designate a function as queueable using the `@queueable` decorator. Currently variable positional arguments (`*args`) and variable keyword arguments (`**kwargs`) are not yet supported. If a function is not marked with the decorator, it cannot be executed via the queue.

```python
from taskqueue import queueable

@queueable
def print_task(txt):
  print(str(txt))
```

You then create queueable instantiations of these functions by using the standard library [`partial`](https://docs.python.org/3/library/functools.html#functools.partial) function to create a concrete binding.

```python
from functools import partial
bound_fn = partial(print_task, txt="hello world")
```

### Old School: RegisteredTask Subclasses

Define a class that inherits from taskqueue.RegisteredTask and implements the `execute` method. RegisteredTasks contain logic that will render their attributes into a JSON payload and can be reconstituted into a live class on the other side of a task queue.

Tasks can be loaded into queues locally or in the cloud and executed later. Here's an example implementation of a trivial `PrintTask`. The attributes of your container class should be simple values that can be easily encoded into JSON such as ints, floats, strings, and numpy arrays. Let the `execute` method download and manipulate heavier data. If you're feeling curious, you can see what JSON a task will turn into by calling `task.payload()`.

```python
from taskqueue import RegisteredTask

class PrintTask(RegisteredTask):
  def __init__(self, txt=''):
    super(PrintTask, self).__init__(txt)
    # attributes passed to super().__init__ are automatically assigned
    # use this space to perform additional processing such as:
    self.txt = str(txt)

  def execute(self):
    if self.txt:
      print(str(self) + ": " + self.txt)
    else:
      print(self)
```

## Local Usage

For small jobs, you might want to use one or more processes to execute the tasks:

```python
from functools import partial
from taskqueue import LocalTaskQueue

tq = LocalTaskQueue(parallel=5) # use 5 processes


tasks = ( PrintTask(i) for i in range(2000) ) # OLD SCHOOL
tasks = ( partial(print_task, i) for i in range(2000) ) # NEW SCHOOL

tq.insert_all(tasks) # performs on-line execution (naming is historical)

# alterternative serial model
tq.insert(tasks)
tq.execute()

# delete tasks
tq.delete(tasks)
tq.purge() # delete all tasks
```

This will load the queue with 1000 print tasks then execute them across five processes.

## Cloud and Cluster Usage

1. Set up an SQS queue and acquire an aws-secret.json that is compatible with CloudVolume. Generate the tasks and insert them into the cloud queue.

2. You can alternatively set up a file based queue that has the same time-based leasing property of an SQS queue.

```python
# import gevent.monkey
# gevent.monkey.patch_all()
from taskqueue import TaskQueue

# region is SQS specific, green means cooperative threading
tq = TaskQueue('sqs://queue-name', region_name="us-east1-b", green=False)
tq = TaskQueue('fq:///path/to/queue/directory/') # file queue ('fq')

# insert accepts any iterable
tq.insert(( PrintTask(i) for i in range(1000) )) # OLD SCHOOL
tq.insert(( partial(print_task, i) for i in range(1000) )) # NEW SCHOOL
tq.enqueued # approximate number of tasks in the queue

# FileQueue Only:
tq.inserted #  total number of tasks inserted
tq.completed # number of tasks completed, requires tally=True with poll
tq.rezero() # reset statistics like inserted and completed
tq.release_all() # set all tasks to available
```

This inserts 1000 PrintTask JSON descriptions into your SQS queue.

Somewhere else, you'll do the following (probably across multiple workers):

```python
from taskqueue import TaskQueue
import MY_MODULE # MY_MODULE contains the definitions of RegisteredTasks

tq = TaskQueue('sqs://queue-name')
tq.poll(
  lease_seconds=int(LEASE_SECONDS),
  verbose=True, # print out a success message
  tally=True, # count number of tasks completed (fq only!)
)
```

Poll will check the queue for a new task periodically. If a task is found, it will execute it immediately, delete the task from the queue, and request another. If no task is found, a random exponential backoff of up to 120sec is built in to prevent workers from attempting to DDOS the queue. If the task fails to complete, the task will eventually recirculate within the queue, ensuring that all tasks will eventually complete provided they are not fundementally flawed in some way.

## Local Container testing

If there is a AWS compatible queue running on a local cluster, e.g. [alpine-sqs](https://hub.docker.com/r/roribio16/alpine-sqs/), the underlying connection client
needs additional parameters. These can be passed into the TaskQueue constructor.

The following code on a worker will work in local and production contexts:

```python
queue = os.environ['SQS_QUEUE']  # for local, set to "default"
region_name = os.environ.get('SQS_REGION_NAME')  # set only for prod
endpoint_url = os.environ.get('SQS_ENDPOINT_URL')  # set only for local
tqueue = taskqueue.TaskQueue(f'sqs://{queue}',
                             region_name=region_name,
                             endpoint_url=endpoint_url)
```

Example docker-compose.yml for local testing:

```yaml
version: "3.7"

services:
  worker:
    image: yourlab/yourworker:v1
    environment:
      - SQS_QUEUE=default
      - SQS_ENDPOINT_URL=http://local_sqs:9324
    depends_on:
      - local_sqs

  local_sqs:
    image: roribio16/alpine-sqs
```

Example docker-compose.yml for production:

```yaml
version: "3.7"

services:
  worker:
    image: yourlab/yourworker:v1
    environment:
      - SQS_QUEUE=my-aws-queue
      - SQS_REGION=us-west-1
```

### Notes on File Queue

```python
# FileQueue Specific Features

tq.inserted # number of inserted tasks
tq.completed # number of completed tasks (counts rerun tasks too)
tq.rezero() # sets tq.inserted and tq.completed to zero.
tq.release_all() # sets all tasks to available
```

FileQueue (`fq://`) is designed to simulate the timed task leasing feature from SQS and exploits a common filesystem to avoid requiring an additional queue server. You can read in detail about its design [on the wiki](https://github.com/seung-lab/python-task-queue/wiki/FileQueue-Design).

There are a few things FileQueue can do that SQS can't and also some quirks you should be aware of. For one, FileQueue can track the number of task completions (`tq.completions`, `tq.poll(..., tally=True)`), but it does so by appending a byte to a file called `completions` for each completion. The size of the file in bytes is the number of completions. This design is an attempt to avoid problems with locking and race conditions. FileQueue also tracks insertions (`tq.insertions`) in a more typical way in an `insertions` file. Also unlike SQS, FileQueue allows listing all tasks at once.

FileQueue also allows releasing all current tasks from their leases, something impossible in SQS. Sometimes a few tasks will die immediately after leasing, but with a long lease, and you'll figure out how to fix them. Instead of starting over or waiting possibly hours, you can set the queue to be made available again (`tq.release_all()`).

As FileQueue is based on the filesystem, it can be managed somewhat via the command line. To delete a queue, just `rm -r $QUEUE_PATH`. To reset a counter: `rm $QUEUE_PATH/completions` (e.g.). If you are brave, you could even use the `mv` command to reassign a task's availability.

We also discovered that FileQueues are also amenable to fixing problems on the fly. In one case, we generated a set of tasks that took 4.5 hours of computation time and decided to run those tasks on a different cluster. The 500k tasks each contained a path to the old storage cluster. Using `find`, `xargs`, and `sed` we were able to fix them efficiently. 

#### Bundled `ptq` CLI Tool 

As of 2.5.0, we now bundle a command line tool `ptq` to make managing running FileQueues easier.

```bash
ptq status fq://./my-queue # prints vital statistics
ptq release fq://./my-queue # releases all tasks from their lease
ptq rezero fq://./my-queue # resets statistics to zero
```

## Motivation

Distributed dependency free task execution engines (such as [Igneous](https://github.com/seung-lab/igneous/)) often make use of cloud based queues like Amazon Simple Queue Service (SQS). In the connectomics field we process petascale images which requires generating hundreds of thousands or millions of cloud tasks per a run. In one case, we were processing serial blocks of a large image where each block depended on the previous block's completion. Each block's run required the generation and upload of millions of tasks and the use of thousands of workers. The workers would rapidly drain the task queue and it was important to ensure that it could be fed fast enough to prevent starvation of this enormous cluster.

There are a few strategies for accomplishing this. One way might be to use a fully featured DAG supporting engine which could generate the next task on demand. However, we were experienced with SQS and had designed our architecture around it. Furthermore, it was, in our experience, robust to thousands of machines knocking on it. This does not discount that there could be better methods out there, but this was convenient for us.

The two major ways to populate the SQS queue at scale would be a task generating task so a single processor could could enlist hundreds or thousands of others or we could just make our task generating client fast and memory efficient and use a handful of cores for multiprocessing. Keeping things simple and local allows for greater operational flexibility and the addition of a drop-in mutiprocessing execution engine allows for the omission of cloud services for small jobs. Importantly, improved small scale performance doesn't preclude the later development of metageneration facilities.

By default, the Python task queue libraries are single threaded and blocking, resulting in upload rates of at most tens of tasks per second. It is possible to do much better by using threads, multiple processes, and by batching requests. TaskQueue has achivied upload rates of over 3000 tasks per second single core, and around 10,000 per second multicore on a single machine. This is sufficient to keep our cluster fed and allows for programmer flexibility as they can populate queues from their local machine using simple scripts.

## How to Achieve High Performance

Attaining the quoted upload rates is simple but takes a few tricks to tune the queue. By default, TaskQueue will upload hundreds of tasks per second using its threading model. We'll show via progressive examples how to tune your upload script to get many thousands of tasks per second with near zero latency and memory usage. Note that the examples below use `sqs://`, but apply to `fq://` as well. These examples also use the old school style of task instantiation, but you can substitute the new style without consequence.

```python
# Listing 1: 10s per second, high memory usage, non-zero latency

tasks = [ PrintTask(i) for i in range(1000000) ]
tq = TaskQueue('sqs://queue-name')
for task in tasks:
  tq.insert(task)
```

This first example shows how you might use the queue in the most naive fashion. The tasks list takes a long time to compute, uses a lot of memory, and then inserts a single task at a time, failing to exploit the threading model in TaskQueue. **Note that this behavior has changed from previous versions where we endorsed the "with" statement where this form was faster, though still problematic.**

```python
# Listing 2: 100-1000s per second, high memory usage, non-zero latency

tasks = [ PrintTask(i) for i in range(1000000) ]
tq = TaskQueue('sqs://queue-name')
tq.insert(tasks)
```

The listing above allows you to use ordinary iterative programming techniques to achieve an upload rate of hundreds per a second without much configuration, a marked improvement over simply using boto nakedly. However, the initial generation of a list of tasks uses a lot of memory and introduces a delay while the list is generated.

This form also takes advantage of SQS batch upload which allows for submitting 10 tasks at once. As the overhead for submitting a task lies mainly in HTTP/1.1 TCP/IP connection overhead, batching 10 requests results in nearly a 10x improvement in performance. However, in this case we've created all the tasks up front again in order to batch them correctly which results in the same memory and latency issues as in Listing 1.

```python
# Listing 3: 100-1000s per second, low memory usage, near-zero latency

tasks = ( PrintTask(i) for i in range(1000000) )
tq = TaskQueue('sqs://queue-name')
tq.insert(tasks, total=1000000) # total necessary for progress bars to work
```

In Listing 3, we've started using generators instead of lists. Generators are essentially lazy-lists that compute the next list element on demand. Defining a generator is fast and takes constant time, so we are able to begin production of new elements nearly instantly. The elements are produced on demand and consumed instantly, resulting in a small constant memory overhead that can be typically measured in kilobytes to megabytes.

As generators do not support the `len` operator, we manually pass in the number of items to display a progress bar.

```python
# Listing 4: 100s-1000s per second, low memory usage, near-zero latency

import gevent.monkey
gevent.monkey.patch_all()
from taskqueue import TaskQueue

tasks = ( PrintTask(i) for i in range(1000000) )
tq = TaskQueue('sqs://queue-name', green=True)
tq.insert(tasks, total=1000000) # total helps the progress bar
```

In Listing 4, we use the `green=True` argument to use cooperative threads. Under the hood, TaskQueue relies on Python kernel threads to achieve concurrent IO. However, on systems with mutliple cores, especially those in a virutalized or NUMA context, the OS will tend to distribute the threads fairly evenly between cores leading to high context-switching overhead. Ironically, a more powerful multicore system can lead to lower performance. To remedy this issue, we introduce a user-space cooperative threading model (green threads) using gevent (which depending on your system is uses either libev or libuv for an event loop).

This can result in a substantial performance increase on some systems. Typically a single core will be fully utilized with extremely low overhead. However, using cooperative threading with networked IO in Python requires monkey patching the standard library (!!). Refusing to patch the standard library will result in single threaded performance. Thus, using GreenTaskQueue can introduce problems into many larger applications (we've seen problems with multiprocessing and ipython). However, often the task upload script can be isolated from the rest of the system and this allows monkey patching to be safely performed. To give users more control over when they wish to accept the risk of monkey patching, it is not performed automatically and a warning will appear with instructions for amending your program.

```python
# Listing 5: 1000s-10000 per second, low memory usage, near zero latency, efficient multiprocessing

import gevent.monkey
gevent.monkey.patch_all()
from taskqueue import TaskQueue
from concurrent.futures import ProcessPoolExecutor

def upload(args):
  start, end = args
  tasks = ( PrintTask(i) for i in range(start, end) )
  tq = TaskQueue('sqs://queue-name', green=True)
  tq.insert(tasks, total=(end - start))

task_ranges = [ (0, 250000), (250000, 500000), (500000, 750000), (750000, 1000000) ]
with ProcessPoolExecutor(max_workers=4) as pool:
  pool.map(upload, task_ranges)
```

In Listing 5, we finally move to multiprocessing to attain the highest speeds. There are three critical pieces of this construction to note.

First, we do not use the usual `multiprocessing` package and instead use `concurrent.futures.ProcessPoolExecutor`. If a child process dies in `multiprocessing`, the parent process will simply hang (this is by design unfortunately...). Using this alternative package, at least an exception will be thrown.

Second, we pass parameters for task generation to the child proceses, not tasks. It is not possible to pass generators from parent to child processes in CPython [1]. It is also inefficient to pass tasks directly as it requires first generating them (as in Listing 1) and then invisibly pickling and unpickling them as they are passed to the child processes. Therefore, we pass only a small number of small picklable objects that are used for constructing a task generator on the other side.

Third, as described in the narrative for Listing 5, the GreenTaskQueue has less context-switching overhead than ordinary multithreaded TaskQueue. Using GreenTaskQueue will cause each core to efficiently run independently of the others. At this point, your main bottlenecks will probably be OS/network card related (let us know if they aren't!). Multiprocessing does scale task production, but it's sub-linear in the number of processes. The task upload rate per a process will fall with each additional core added, but each core still adds additional throughput up to some inflection point.

```python
# Listing 6: Exchanging Generators for Iterators

import gevent.monkey
gevent.monkey.patch_all()
from taskqueue import TaskQueue
from concurrent.futures import ProcessPoolExecutor

class PrintTaskIterator(object):
  def __init__(self, start, end):
    self.start = start
    self.end = end
  def __len__(self):
    return self.end - self.start
  def __iter__(self):
    for i in range(self.start, self.end):
      yield PrintTask(i)

def upload(tsks):
  tq = TaskQueue('sqs://queue-name', green=True)
  tq.insert(tsks)

tasks = [ PrintTaskIterator(0, 100), PrintTaskIterator(100, 200) ]
with ProcessPoolExecutor(max_workers=2) as execute:
  execute.map(upload, tasks)
```

If you insist on wanting to pass generators to your subprocesses, you can use iterators instead. The construction above allows us to write the generator call up front, pass only a few primatives through the pickling process, and transparently call the generator on the other side. We can even support the `len()` function which is not available for generators.

```python
# Listing 7: Easy Multiprocessing

import gevent.monkey
gevent.monkey.patch_all(thread=False)
import copy
from taskqueue import TaskQueue

class PrintTaskIterator(object):
  def __init__(self, start, end):
    self.start = start
    self.end = end
  def __getitem__(self, slc):
    itr = copy.deepcopy(self)
    itr.start = self.start + slc.start
    itr.end = self.start + slc.stop
    return itr
  def __len__(self):
    return self.end - self.start
  def __iter__(self):
    for i in range(self.start, self.end):
      yield PrintTask(i)

tq = TaskQueue('sqs://queue-name', green=True)
tq.insert(PrintTaskIterator(0,200), parallel=2)
```

If you design your iterators such that the slice operator works, TaskQueue can
automatically resection the iterator such that it can be fed to multiple processes. Notably, we don't return `PrintTaskIterator(self.start+slc.start, self.start+slc.stop)` because it triggers an endless recursion during pickling. However, the runtime copy implementation above sidesteps this issue. Internally, `PrintTaskIterator(0,200)` will be turned into `[ PrintTaskIterator(0,100), PrintTaskIterator(100,200) ]`. We also perform tracking of exceptions raised by child processes in a queue. `gevent.monkey.patch_all(thread=False)` was necessary to avoid multiprocess hanging.

[1] You can't pass generators in CPython but [you can pass iterators](https://stackoverflow.com/questions/1939015/singleton-python-generator-or-pickle-a-python-generator/1939493#1939493). You can pass generators if you use Pypy or Stackless Python.

--  
Made with <3.
