import gevent.monkey
gevent.monkey.patch_all()

from .registered_task import RegisteredTask, MockTask, PrintTask
from .taskqueue import TaskQueue, MockTaskQueue, LocalTaskQueue, upload
from .secrets import (
  QUEUE_NAME, TEST_QUEUE_NAME, QUEUE_TYPE, 
  PROJECT_NAME, AWS_DEFAULT_REGION
)

__version__ = '0.12.3'