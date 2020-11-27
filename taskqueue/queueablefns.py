from collections import namedtuple
import functools
import inspect
import orjson
import copy
import re
from collections import OrderedDict
from functools import partial

import numpy as np

REGISTRY = {}
FunctionTask = namedtuple("FunctionTask", [ "key", "args", "kwargs", "id" ])

class UnregisteredFunctionError(BaseException):
  pass

def totask(data, ident=-1):
  if isinstance(data, FunctionTask):
    return data
  if isinstance(data, partial) or callable(data):
    return serialize(data)

  if type(data) is bytes:
    data = data.decode('utf8')
  if isinstance(data, six.string_types):
    data = orjson.loads(data)
  data[3] = ident
  return FunctionTask(*data) 

def tofunc(task):
  if callable(task):
    return task

  fn = REGISTRY.get(task[0], None)
  if fn is None:
    raise UnregisteredFunctionError("{} is not registered as a queuable function.".format(task.key))
  return partial(fn, *task[1], **task[2])

def execute(task):
  tofunc(task)()

# class FunctionTask():
#   __slots__ = ['key', 'args', 'kwargs', 'id']
#   def __init__(self, key, args, kwargs, id=None):
#     self.key = tuple(key)
#     self.args = args
#     self.kwargs = kwargs
#     self.id = id
#     self._order = ('key', 'args', 'kwargs', 'id')
#   def __getitem__(self, idx):
#     return getattr(self, self._order[idx])
#   def __setitem__(self, idx, value):
#     setattr(self, self._order[idx], value)
#   def serialize(self):
#     return serialize(partial(REGISTRY[self.key], *self.args, **self.kwargs))
#   def execute(self):
#     self()
#   def __call__(self):
#     function(self)()

def jsonifyable(obj):
  if hasattr(obj, 'serialize') and callable(obj.serialize):
    return obj.serialize()

  try:
    iter(obj)
  except TypeError:
    return obj

  if isinstance(obj, bytes):
    return obj.decode('utf8')
  elif isinstance(obj, str):
    return obj

  if isinstance(obj, list) or isinstance(obj, tuple):
    return [ jsonifyable(x) for x in obj ] 

  for key, val in six.iteritems(obj):
    if isinstance(val, np.ndarray):
      obj[key] = val.tolist()
    elif isinstance(val, dict):
      obj[key] = jsonifyable(val)
    elif isinstance(val, list):
      obj[key] = [ jsonifyable(x) for x in val ]
    elif hasattr(val, 'serialize') and callable(val.serialize):
      obj[key] = val.serialize()

  return obj

def argsokay(fn, args, kwargs):
  spec = inspect.getfullargspec(fn)

  kwargct = 0
  sargs = set(spec.args)
  for name in kwargs.keys():
    kwargct += (name in sargs)

  if len(spec.args) - len(spec.defaults) <= len(args) + kwargct <= len(spec.args):
    return False

  for name in kwargs.keys():
    if not (name in sargs) and not (name in spec.kwonlyargs):
      return False

  return True

def serialize(fn, id=None):
  if isinstance(fn, FunctionTask):
    return fn.serialize()

  if not isinstance(fn, partial) and not callable(fn):
    raise ValueError("Must be a partial or other callable.")

  args = []
  kwargs = {}
  rawfn = fn
  while isinstance(rawfn, partial):
    args += rawfn.args
    kwargs.update(rawfn.keywords)
    rawfn = rawfn.func

  if not argsokay(rawfn, args, kwargs):
    raise ValueError("{} didn't get valid arguments. Got: {}, {}. Expected: {}".format(
      rawfn, args, kwargs, inspect.getfullargspec(rawfn)
    ))

  return FunctionTask(
    (rawfn.__module__, rawfn.__name__), 
    jsonifyable(args), 
    jsonifyable(kwargs),
    id
  )

def deserialize(data):
  if type(data) is bytes:
    data = data.decode('utf8')
  if isinstance(data, six.string_types):
    data = orjson.loads(data)
  return FunctionTask(*data)

def queueable(fn):
  REGISTRY[(fn.__module__, fn.__name__)] = fn
  return fn