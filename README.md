[![Build Status](https://travis-ci.org/seung-lab/python-task-queue.svg?branch=master)](https://travis-ci.org/seung-lab/python-task-queue) [![PyPI version](https://badge.fury.io/py/task-queue.svg)](https://badge.fury.io/py/task-queue) 

# python-task-queue

This package provides a client and system for generating, uploading, leasing, and executing dependency free tasks both locally and in the cloud using AWS SQS.

## Installation

```bash
pip install numpy # make sure you do this first on a seperate line
pip install task-queue
```

The task queue uses your CloudVolume secrets located in `$HOME/.cloudvolume/secrets/`. When using AWS SQS as your queue backend, you must provide `$HOME/.cloudvolume/secrets/aws-secret.json`. See the [CloudVolume](https://github.com/seung-lab/cloud-volume) repo for additional instructions.  

## Usage 

Define a class that inherits from taskqueue.RegisteredTask and implments the `execute` method. RegisteredTasks contain logic that will render their attributes into a JSON payload and can be reconstituted into a live class on the other side of a task queue.  

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
from taskqueue import LocalTaskQueue

with LocalTaskQueue(parallel=5) as tq: # use 5 processes
  for _ in range(1000):
    tq.insert(
      PrintTask(i)
    )
```
This will load the queue with 1000 print tasks then execute them across five processes.

## Cloud Usage

Set up an SQS queue and acquire an aws-secret.json that is compatible with CloudVolume. Generate the tasks and insert them into the cloud queue. 

```python
from taskqueue import TaskQueue

# new and fast
tq = TaskQueue('sqs-queue-name')
tq.insert_all(( PrintTask(i) for i in range(1000) )) # can use lists or generators

# older 10x slower alternative
with TaskQueue('sqs-queue-name') as tq:
  for i in range(1000):
    tq.insert(PrintTask(i))
```

This inserts 1000 PrintTask JSON descriptions into your SQS queue.

Somewhere else, you'll do the following (probably across multiple workers):

```python
from taskqueue import TaskQueue
import MY_MODULE # MY_MODULE contains the definitions of RegisteredTasks

tq = TaskQueue('sqs-queue-name')
tq.poll(lease_Seconds=int(LEASE_SECONDS))
```

Poll will check SQS for a new task periodically. If a task is found, it will execute it immediately, delete the task from the queue, and request another. If no task is found, a random exponential backoff of up to 60sec is built in to prevent workers from attempting to DDOS the queue. If the task fails to complete, the task will eventually recirculate within the SQS queue, ensuring that all tasks will eventually complete provided they are not fundementally flawed in some way.

## Motivation

Simple (i.e. dependency free) distributed task execution engines (such as [Igneous](https://github.com/seung-lab/igneous/)) often make use of cloud based queues like Amazon Simple Queue Service (SQS). In the connectomics field we process petascale images which requires generating hundreds of thousands or millions of cloud tasks per a run. In one case, we were processing serial blocks of a large image where each block depended on the previous block's completion. Each block's run required the generation and upload of millions of tasks and the use of thousands of workers. The workers would rapidly drain the task queue and it was important to ensure that it could be fed fast enough to prevent starvation of this enormous cluster.  

There are a few strategies for accomplishing this. One way might be to use a fully featured DAG supporting engine which potentially could generate the next task on demand. However, we were experienced with SQS and had designed our architecture around it. Furthermore, it was, in our experience, robust to thousands of machines knocking on it. This does not discount that there are probably better methods out there, merely that this was convenient for us.

Accepting this constraint, the two major ways to populate the SQS queue would be a task generating task that could enlist hundreds or thousands of processors or we could just make our task generating client fast and memory efficient and use a handful of cores for multiprocessing. Keeping things simple and local allows for greater operational flexibility and the addition of a drop in mutiprocessing execution engine allows for the omission of cloud services for small jobs without heavily modifying the architecture.  

By default, the Python task queue libraries are single threaded and blocking, resulting in upload rates of at most tens of tasks per second. It is possible to do much better by using threads, mutliple processes, and batching requests. TaskQueue has been witnessed achiving upload rates of over 3000 tasks per second single core, and around 10,000 per second multicore on a single machine. This is sufficient to keep a huge cluster fed and allows for enormous programmer flexibility as they can populate queues from their local machine using simple scripts.

## How to Achieve High Performance

Attaining the above quoted upload rates is fairly simple but takes a few tricks to tune the queue. By default, TaskQueue will provide upload rates of hundreds of tasks per second using its threading model. We'll show via progressive examples how to tune your upload script to get many thousands of tasks per second with near zero latency and memory usage.

```python 
# Listing 1: 100s per second, high memory usage, non-zero latency

tasks = [ PrintTask(i) for i in range(1000000) ]
with TaskQueue('sqs-queue-name') as tq:
  for task in tasks:
    tq.insert(task)
```

The listing above allows you to use ordinary iterative programming techniques to achieve an upload rate of hundreds per a second without much configuration, a marked improvement over simply using boto nakedly. However, the initial generation of a list of tasks uses a lot of memory and introduces a delay while the list is generated. 

```python 
# Listing 2: 100s per second, usually low memory usage, near-zero latency

with TaskQueue('sqs-queue-name') as tq:
  for i in range(1000000):
    tq.insert(PrintTask(i))
``` 

Listing 2 generates tasks and begins uploading them simultaneously. Initially, this results in low memory usage, but sometimes task generation begins to outpace uploading and causes memory usage to grow. Regardless, upload begins immediately so no latency is introduced. 


```python 
# Listing 3: 100-1000s per second, high memory usage, non-zero latency

tasks = [ PrintTask(i) for i in range(1000000) ]
with TaskQueue('sqs-queue-name') as tq:
  tq.insert_all(tasks)
```

Listing 3 takes advantage of SQS batch upload which allows for submitting 10 tasks at once. As the overhead for submitting a task lies mainly in HTTP/1.1 TCP/IP connection overhead, batching 10 requests results in nearly a 10x improvement in performance. However, in this case we've created all the tasks up front again in order to batch them correctly which results in the same issues as in Listing 1. 


```python 
# Listing 4: 100s-1000s per second, low memory usage, near-zero latency 

tasks = ( PrintTask(i) for i in range(1000000) ) 
with TaskQueue('sqs-queue-name') as tq:
  tq.insert_all(tasks)
```

In Listing 4, we've started using generators instead of lists. Generators are essentially lazy-lists that compute the next list element on demand. The definition of a generator is fast and takes constant time, so we are able to begin production of new elements nearly instantly. The elements are produced on demand and consumed instantly, resulting in a small constant memory overhead that can be typically measured in kilobytes to megabytes.  

```python 
# Listing 5: 100s-1000s per second, low memory usage, near-zero latency
import gevent.monkey 
gevent.monkey.patch_all()
from taskqueue import GreenTaskQueue 

tasks = ( PrintTask(i) for i in range(1000000) ) 
with GreenTaskQueue('sqs-queue-name') as tq:
  tq.insert_all(tasks)
```

In Listing 5, we replace TaskQueue with GreenTaskQueue. Under the hood, TaskQueue relies on Python kernel threads to achieve concurrent IO. However, on systems with mutliple cores, especially those in a virutalized or NUMA context, the OS will tend to distribute the threads fairly evenly between cores leading to high context-switching overhead. Ironically, a more powerful multicore system can lead to lower performance. To remedy this issue, we introduce a user-space cooperative threading model (green threads) using gevent (which depending on your system is uses either libev or libuv for an event loop).  

This can result in substantial performance increase on some systems as typically a single core will be fully utilized with extremely low overhead. However, using cooperative threading with networked IO in Python requires monkey patching the standard library (!!). Refusing to patch the standard library will result in single threaded performance. This can introduce problems into many larger applications (we've seen problems sometimes where multiprocessing will hang and it's incompatible with ipython). However, often the task upload script can be isolated from the rest of the system and monkey patching safely performed. Because of the risk, monkey patching is not performed automatically and a warning will appear if you don't do it manually. 

```python 
# Listing 6: 1000s-10000 per second, low memory usage, near zero latency, efficient multiprocessing
import gevent.monkey 
gevent.monkey.patch_all()
from taskqueue import GreenTaskQueue 
from concurrent.futures import ProcessPoolExecutor 

def upload(args):
  start, end = args
  tasks = ( PrintTask(i) for i in range(start, end) ) 
  with GreenTaskQueue('sqs-queue-name') as tq:
    tq.insert_all(tasks) 

task_ranges = [ (0, 250000), (250000, 500000), (500000, 750000), (750000, 1000000) ]
with ProcessPoolExecutor(max_workers=4) as pool:
  pool.map(upload, task_ranges)
``` 

In Listing 6, we finally move to multiprocessing to attain the highest speeds. There are three critical pieces of this construction to note.   

First, we do not use the usual `multiprocessing` package and instead use `concurrent.futures.ProcessPoolExecutor`. If a child process dies in `multiprocessing`, the parent process will simply hang (this is by design unfortunately...). Using this alternative package, at least an exception will be thrown.  

Second, we pass parameters for task generation to the child proceses, not tasks. It is not possible to pass generators from parent to child processes. It is also inefficient to pass tasks directly as it requires first generating them (as in Listing 1) and then invisibly pickling and unpickling them as they are passed to the child processes. Therefore, we pass only a small number of small picklable objects that are used for constructing a task generator on the other side.   

Third, as described in the narrative for Listing 5, the GreenTaskQueue has less context-switching overhead than ordinary multithreaded TaskQueue. Using GreenTaskQueue will cause each core to efficiently run independently of the others. At this point, your main bottlenecks will probably be OS/network card related (let us know if they aren't!). Multiprocessing does scale task production, but it's not 1:1 in the number of processes. The number of tasks per a process will fall with each additional core added, but each core still adds additional throughput up to about 16 cores.

**--**  
**Made with <3.**

