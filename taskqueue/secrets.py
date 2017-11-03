from __future__ import print_function

import os
import json

from google.oauth2 import service_account
from cloudvolume.lib import mkdir

QUEUE_NAME = '' if 'PIPELINE_USER_QUEUE' not in os.environ else os.environ['PIPELINE_USER_QUEUE']
TEST_QUEUE_NAME = '' if 'TEST_PIPELINE_USER_QUEUE' not in os.environ else os.environ['TEST_PIPELINE_USER_QUEUE']
QUEUE_TYPE = 'pull-queue' if 'QUEUE_TYPE' not in os.environ else os.environ['QUEUE_TYPE']
PROJECT_NAME = 'neuromancer-seung-import' 
APPENGINE_QUEUE_URL = 'https://queue-dot-neuromancer-seung-import.appspot.com'

CLOUD_VOLUME_DIR = mkdir(os.path.join(os.environ['HOME'], '.cloudvolume'))

def secretpath(filepath):
  preferred = CLOUD_VOLUME_DIR
  
  if os.path.exists(preferred):
    return os.path.join(preferred, filepath)

  backcompat = [
    os.path.join(os.environ['HOME'], '.neuroglancer'), # older
    '/' # original
  ]

  for path in rootpaths:
    if os.path.exists(path):
      print(colorize('yellow', 'Deprecation Warning: Directory ~/.cloudvolume is now preferred to ~/.neuroglancer.\nConsider running: mv ~/.neuroglancer ~/.cloudvolume'))  
      return mkdir(os.path.join(path, filepath))

  return mkdir(os.path.join(preferred, filepath))

project_name_path = secretpath('project_name')
if os.path.exists(project_name_path):
  with open(project_name_path, 'r') as f:
    PROJECT_NAME = f.read()

google_credentials_path = secretpath('secrets/google-secret.json')
if os.path.exists(google_credentials_path):
  google_credentials = service_account.Credentials \
    .from_service_account_file(google_credentials_path)
else:
  google_credentials = ''

