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
    cls._arg_names = inspect.getfullargspec(class_dict['__init__'])[0][1:]
    return cls

class RegisteredTask(with_metaclass(Meta)):
  def __init__(self, *args, **kwargs):
    self._args = OrderedDict(zip(self._arg_names, args))
    self._args.update(kwargs)
  
  @classmethod
  def deserialize(cls, data):
    obj = deserialize(data)
    assert isinstance(obj, cls)
    return obj

  def payload(self):
    def denumpy(dictionary):
      for key, val in six.iteritems(dictionary):
        if isinstance(val, np.ndarray):
          dictionary[key] = val.tolist()
        elif isinstance(val, dict):
          dictionary[key] = denumpy(val)
      return dict(dictionary)

    argcpy = copy.deepcopy(self._args)
    argcpy['class'] = self.__class__.__name__

    return denumpy(argcpy)

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
    super(PrintTask, self).__init__()
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