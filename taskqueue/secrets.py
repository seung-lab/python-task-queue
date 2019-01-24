from __future__ import print_function

import os
import json

from cloudvolume.lib import mkdir, colorize
from cloudvolume.secrets import (
  CLOUD_VOLUME_DIR, PROJECT_NAME, 
  aws_credentials, aws_credentials_path, 
  boss_credentials, boss_credentials_path
)

QUEUE_NAME = 'pull-queue' if 'PIPELINE_USER_QUEUE' not in os.environ else os.environ['PIPELINE_USER_QUEUE']
TEST_QUEUE_NAME = 'test-pull-queue' if 'TEST_PIPELINE_USER_QUEUE' not in os.environ else os.environ['TEST_PIPELINE_USER_QUEUE']
QUEUE_TYPE = 'sqs' if 'QUEUE_TYPE' not in os.environ else os.environ['QUEUE_TYPE']
AWS_DEFAULT_REGION = 'us-east-1' if 'AWS_DEFAULT_REGION' not in os.environ else os.environ['AWS_DEFAULT_REGION']

