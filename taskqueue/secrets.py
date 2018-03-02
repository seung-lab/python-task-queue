from __future__ import print_function

import os
import json

from google.oauth2 import service_account
from cloudvolume.lib import mkdir, colorize
from cloudvolume.secrets import (
  CLOUD_VOLUME_DIR, PROJECT_NAME, 
  google_credentials_path, google_credentials,
  aws_credentials, aws_credentials_path, 
  boss_credentials, boss_credentials_path
)

QUEUE_NAME = 'pull-queue' if 'PIPELINE_USER_QUEUE' not in os.environ else os.environ['PIPELINE_USER_QUEUE']
TEST_QUEUE_NAME = 'test-pull-queue' if 'TEST_PIPELINE_USER_QUEUE' not in os.environ else os.environ['TEST_PIPELINE_USER_QUEUE']
QUEUE_TYPE = 'pull-queue' if 'QUEUE_TYPE' not in os.environ else os.environ['QUEUE_TYPE']
APPENGINE_QUEUE_URL = 'https://queue-dot-neuromancer-seung-import.appspot.com'



# google_credentials_path = secretpath('secrets/google-secret.json')
# if os.path.exists(google_credentials_path):
#   google_credentials = service_account.Credentials \
#     .from_service_account_file(google_credentials_path, scopes=[
#     	'https://www.googleapis.com/auth/appengine.admin', 
#     	'https://www.googleapis.com/auth/cloud-platform', 
#     	'https://www.googleapis.com/auth/cloud-tasks'
#     ])
# else:
#   google_credentials = ''
