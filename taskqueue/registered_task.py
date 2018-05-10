import six
from six import with_metaclass
import inspect
import json
import base64
import copy
import re
from collections import OrderedDict
from functools import partial

import numpy as np

registry = {}

def register_class(target_class):
    registry[target_class.__name__] = target_class

def deserialize(data):
    params = json.loads(data)
    name = params['class']
    target_class = registry[name]
    del params['class']
    return target_class(**params)

def payloadBase64Decode(payload):
    decoded_string = base64.b64decode(payload).decode('utf8')
    return deserialize(decoded_string)

class Meta(type):
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        register_class(cls)
        cls._arg_names = inspect.getargspec(class_dict['__init__'])[0][1:]
        return cls

class RegisteredTask(with_metaclass(Meta)):
    def __init__(self, *arg_values):
        self._args = OrderedDict(zip(self._arg_names, arg_values))

    @classmethod
    def deserialize(cls, base64data):
        obj = deserialize(base64data)
        assert type(obj) == cls
        return obj
        
    def serialize(self):
        d = copy.deepcopy(self._args)
        d['class'] = self.__class__.__name__

        def serializenumpy(obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj

        return json.dumps(d, default=serializenumpy)

    @property
    def payloadBase64(self):
        return base64.b64encode(self.serialize().encode('utf8'))

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
    def __init__(self, *args):
        super(MockTask, self).__init__()
    def execute(self):
        pass