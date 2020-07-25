from collections import namedtuple
import os
import posixpath
import re
import sys

from .lib import yellow, toabs

ExtractedPath = namedtuple('ExtractedPath', 
  ('protocol', 'path')
)

ALLOWED_PROTOCOLS = [ 'sqs', 'fq', 'mem' ]

def mkpath(extracted_path):
  return extracted_path.protocol + "://" + extracted_path.path

def pop_protocol(cloudpath):
  protocol_re = re.compile(r'(\w+)://')

  match = re.match(protocol_re, cloudpath)

  if not match:
    return ('sqs', cloudpath)

  (protocol,) = match.groups()
  cloudpath = re.sub(protocol_re, '', cloudpath, count=1)

  return (protocol, cloudpath)

def extract_path(cloudpath):
  protocol, queue_path = pop_protocol(cloudpath)
  if protocol in ('http', 'https'):
    if 'sqs' in queue_path and 'amazonaws.com' in queue_path:
      protocol = 'sqs'
    queue_path = cloudpath
    
  return ExtractedPath(protocol, queue_path)

