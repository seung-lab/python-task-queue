from functools import partial

import orjson

from .queueablefns import totask as qtotask, FunctionTask, FunctionTaskLite, tofunc
from .registered_task import totask as rtotask, RegisteredTask

def totask(task):
  if isinstance(task, (FunctionTask, RegisteredTask)):
    return task

  if type(task) is bytes:
    task = task.decode('utf8')
  if isinstance(task, str):
    task = orjson.loads(task)

  ident = -1
  if isinstance(task, dict):
    ident = task.get('id', -1)
    if 'payload' in task:
      task = task['payload']

  if isinstance(task, FunctionTaskLite):
    return FunctionTask(*task)
  elif isinstance(task, list):
    task[3] = ident
    return FunctionTask(*task)
  elif isinstance(task, dict):
    return rtotask(task, ident)
  elif isinstance(task, partial) or callable(task):
    return qtotask(task, ident)

  raise ValueError("Unable to convert {} into a task object.".format(task))

def totaskid(taskid):
  if hasattr(taskid, 'id'):
    return taskid.id
  elif 'id' in taskid:
    return taskid['id']
  elif isinstance(taskid, (list, tuple)):
    return taskid[3]
  return taskid