from .registered_task import RegisteredTask, MockTask, PrintTask
from .taskqueue import (
  TaskQueue, MockTaskQueue, GreenTaskQueue, LocalTaskQueue, 
  multiprocess_upload, QueueEmptyError, totask, UnsupportedProtocolError
)

__version__ = '2.6.0'