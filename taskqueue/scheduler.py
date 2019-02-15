import sys

from concurrent.futures import ThreadPoolExecutor

import gevent.pool
import gevent.monkey
from tqdm import tqdm

from cloudvolume.lib import yellow

def schedule_threaded_jobs(
    fns, concurrency=40, 
    progress=None, total=None, batch_size=1,
  ):
  
  pbar = tqdm(total=total, desc=progress, disable=(not progress))
    
  def updatefn(fn):
    def realupdatefn():
      res = fn()
      pbar.update(batch_size)
    return realupdatefn

  with ThreadPoolExecutor(max_workers=concurrency) as executor:
    for fn in fns:
      executor.submit(updatefn(fn))

  pbar.close()

def schedule_green_jobs(
    fns, concurrency=40, 
    progress=None, total=None, batch_size=1,
  ):

  if total is None:
    try:
      total = len(fns)
    except TypeError: # generators don't have len
      pass

  pbar = tqdm(total=total, desc=progress, disable=(not progress))
  results = []
  
  def updatefn(fn):
    def realupdatefn():
      res = fn()
      pbar.update(batch_size)
      results.append(res)
    return realupdatefn

  pool = gevent.pool.Pool(concurrency)
  for fn in fns:
    pool.spawn( updatefn(fn) )

  pool.join()
  pool.kill()
  pbar.close()

  return results

