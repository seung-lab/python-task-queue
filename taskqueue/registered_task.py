import six
from six import with_metaclass
import inspect
import json
import copy
import re
from collections import OrderedDict
from functools import partial

import numpy as np

REGISTRY = {}

def totask(task):
  if isinstance(task, RegisteredTask):
    return task

  if type(task) is bytes:
    task = task.decode('utf8')

  if isinstance(task, six.string_types):
    task = json.loads(task)

  ident = task.get('id', -1)

  if 'payload' in task:
    task = task['payload']

  taskobj = deserialize(task)
  taskobj._id = ident
  return taskobj

def totaskid(taskid):
  if isinstance(taskid, RegisteredTask):
    return taskid.id
  elif 'id' in taskid:
    return taskid['id']
    
  return taskid

def deserialize(data):
  if type(data) is bytes:
    data = data.decode('utf8')

  if isinstance(data, six.string_types):
    data = json.loads(data)

  name = data['class']
  target_class = REGISTRY[name]
  del data['class']
  return target_class(**data)

class Meta(type):
  def __new__(meta, name, bases, class_dict):
    cls = type.__new__(meta, name, bases, class_dict)
    REGISTRY[cls.__name__] = cls

    if hasattr(inspect, 'getfullargspec'):
      argspecfn = inspect.getfullargspec
    else:
      argspecfn = inspect.getargspec 

    cls._arg_names = argspecfn(class_dict['__init__'])[0][1:]
    return cls

class RegisteredTask(with_metaclass(Meta)):
  def __init__(self, *args, **kwargs):
    self._args = OrderedDict(zip(self._arg_names, args))
    self._args.update(kwargs)

    for k,v in six.iteritems(self._args):
      self.__dict__[k] = v
  
  @classmethod
  def deserialize(cls, data):
    obj = deserialize(data)
    assert isinstance(obj, cls)
    return obj

  def payload(self):
    def denumpy(obj):
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
        return [ denumpy(x) for x in obj ] 

      for key, val in six.iteritems(obj):
        if isinstance(val, np.ndarray):
          obj[key] = val.tolist()
        elif isinstance(val, dict):
          obj[key] = denumpy(val)
        elif isinstance(val, list):
          obj[key] = [ denumpy(x) for x in val ]
        elif hasattr(val, 'serialize') and callable(val.serialize):
          obj[key] = val.serialize()

      return obj

    argcpy = copy.deepcopy(self._args)
    for k,v in six.iteritems(self._args):
      argcpy[k] = self.__dict__[k]
    argcpy['class'] = self.__class__.__name__

    return dict(denumpy(argcpy))

  @property
  def id(self):
    return self._id

  def __repr__(self):
    string = self.__class__.__name__ + "("
    for arg_name, arg_value in six.iteritems(self._args):
      if isinstance(arg_value, six.string_types):
        string += "{}='{}',".format(arg_name, arg_value)
      else:
        string += "{}={},".format(arg_name, arg_value)

    # remove the last comma if necessary
    if string[-1] == ',':
      string = string[:-1]

    return string + ")"  

class PrintTask(RegisteredTask):
  def __init__(self, txt=''):
    super(PrintTask, self).__init__(txt)
    self.txt = txt

  def execute(self):
    if self.txt:
      print(str(self) + ": " + str(self.txt))
    else:
      print(self)

class MockTask(RegisteredTask):
  def __init__(self, **kwargs):
    super(MockTask, self).__init__(**kwargs)
  def execute(self):
    pass