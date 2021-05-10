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
FunctionTaskLite = namedtuple("FunctionTaskLite", [ "key", "args", "kwargs", "id" ])

class UnregisteredFunctionError(BaseException):
  pass

def totask(data, ident=-1):
  if isinstance(data, FunctionTask):
    return data
  if isinstance(data, partial) or callable(data):
    return func2task(data, ident)

  if type(data) is bytes:
    data = data.decode('utf8')
  if isinstance(data, str):
    data = orjson.loads(data)
  data[3] = ident
  return FunctionTask(*data) 

def func2task(fn, ident):
  args = []
  kwargs = {}
  rawfn = fn
  while isinstance(rawfn, partial):
    args += rawfn.args
    kwargs.update(rawfn.keywords)
    rawfn = rawfn.func

  if not argsokay(rawfn, args, kwargs):
    raise TypeError("{} didn't get valid arguments. Got: {}, {}. Expected: {}".format(
      rawfn, args, kwargs, inspect.getfullargspec(rawfn)
    ))

  return FunctionTask(
    (rawfn.__module__, rawfn.__name__), 
    args, 
    kwargs,
    ident
  )

def tofunc(task):
  if callable(task):
    return task

  fn = REGISTRY.get(tuple(task[0]), None)
  if fn is None:
    raise UnregisteredFunctionError("{} is not registered as a queuable function.".format(task.key))
  return partial(fn, *task[1], **task[2])

class FunctionTask():
  __slots__ = ['key', 'args', 'kwargs', 'id', '_order']
  def __init__(self, key, args, kwargs, id=None):
    self.key = tuple(key)
    self.args = args
    self.kwargs = kwargs
    self.id = id
    self._order = ('key', 'args', 'kwargs', 'id')
  def __getitem__(self, idx):
    return getattr(self, self._order[idx])
  def __setitem__(self, idx, value):
    setattr(self, self._order[idx], value)
  def __iter__(self):
    raise TypeError("FunctionTask is not an iterable.")
  def payload(self):
    return FunctionTaskLite(self.key, self.args, self.kwargs, self.id)
  def execute(self, *args, **kwargs):
    self(*args, **kwargs)
  def tofunc(self):
    fn = REGISTRY.get(tuple(self.key), None)
    if fn is None:
      raise UnregisteredFunctionError("{} is not registered as a queuable function.".format(self.key))
    return partial(fn, *self.args, **self.kwargs)
  def __repr__(self):
    return "FunctionTask({},{},{},\"{}\")".format(self.key, self.args, self.kwargs, self.id)
  def __call__(self):
    return self.tofunc()()

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

  for key, val in obj.items():
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
  sig = inspect.signature(fn)
  try:
    sig.bind(*args, **kwargs)
  except TypeError:
    return False
  return True

def queueable(fn):
  """Register the input function as queueable and executable via TaskQueue."""
  REGISTRY[(fn.__module__, fn.__name__)] = fn
  return fn