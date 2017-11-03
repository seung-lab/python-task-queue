from __future__ import print_function

import os
import json

from google.oauth2 import service_account
from cloudvolume.lib import mkdir, colorize

QUEUE_NAME = '' if 'PIPELINE_USER_QUEUE' not in os.environ else os.environ['PIPELINE_USER_QUEUE']
TEST_QUEUE_NAME = '' if 'TEST_PIPELINE_USER_QUEUE' not in os.environ else os.environ['TEST_PIPELINE_USER_QUEUE']
QUEUE_TYPE = 'pull-queue' if 'QUEUE_TYPE' not in os.environ else os.environ['QUEUE_TYPE']
PROJECT_NAME = 'neuromancer-seung-import' 
APPENGINE_QUEUE_URL = 'https://queue-dot-neuromancer-seung-import.appspot.com'

CLOUD_VOLUME_DIR = mkdir(os.path.join(os.environ['HOME'], '.cloudvolume'))

def secretpath(filepath):
  preferred = os.path.join(CLOUD_VOLUME_DIR, filepath)
  
  if os.path.exists(preferred):
    return preferred

  backcompat = [
    os.path.join(os.environ['HOME'], '.neuroglancer'), # older
    '/' # original
  ]

  backcompat = [ os.path.join(path, filepath) for path in backcompat ] 

  for path in backcompat:
    if os.path.exists(path):
      print(colorize('yellow', 'Deprecation Warning: {} is now preferred to {}.'.format(preferred, path)))  
      return path

  return preferred

project_name_path = secretpath('project_name')
if os.path.exists(project_name_path):
  with open(project_name_path, 'r') as f:
    PROJECT_NAME = f.read().strip()

google_credentials_path = secretpath('secrets/google-secret.json')
if os.path.exists(google_credentials_path):
  google_credentials = service_account.Credentials \
    .from_service_account_file(google_credentials_path)
else:
  google_credentials = ''

