from taskqueue import PrintTask

import copy

def crt_tasks(a,b):
  bounds = 5

  class TaskIterator():
    def __init__(self, x):
      self.x = x 
    def __len__(self):
      return b-a
    def __getitem__(self, slc):
      itr = copy.deepcopy(self)
      itr.x = 666
      return itr
    def __iter__(self):
      for i in range(a,b):
        yield PrintTask(str(i) + str(self.x))

  return TaskIterator(bounds)
