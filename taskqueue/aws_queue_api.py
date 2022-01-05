import json
import re
import types

import boto3
import botocore.exceptions
import botocore.errorfactory

from .lib import toiter, sip, jsonify
from .secrets import aws_credentials
from .secrets import (
  AWS_DEFAULT_REGION
)

import tenacity

AWS_BATCH_SIZE = 10 # send_message_batch's max batch size is 10

class ClientSideError(Exception):
  pass

retry = tenacity.retry(
  reraise=True,
  stop=tenacity.stop_after_attempt(4),
  wait=tenacity.wait_random_exponential(0.5, 60.0),
  retry=tenacity.retry_if_not_exception_type(ClientSideError),
)

class AWSTaskQueueAPI(object):
  def __init__(self, qurl, region_name=AWS_DEFAULT_REGION, **kwargs):
    """
    qurl: either a queue name (e.g. 'pull_queue') or a url
      like https://sqs.us-east-1.amazonaws.com/DIGITS/wms-pull-queue
    kwargs: Keywords for the underlying boto3.client constructor, other than `service_name`, 
      `region_name`, `aws_secret_access_key`, or `aws_access_key_id`.
    """
    if 'region' in kwargs:
      region_name = kwargs.pop('region')
    
    matches = re.search(r'sqs.([\w\d-]+).amazonaws', qurl)

    if matches is not None:
      region_name, = matches.groups()
      self.qurl = qurl
    else:
      self.qurl = None

    credentials = aws_credentials()
    
    self.sqs = boto3.client('sqs', 
      aws_secret_access_key=credentials.get('AWS_SECRET_ACCESS_KEY'),
      aws_access_key_id=credentials.get('AWS_ACCESS_KEY_ID'),
      region_name=region_name,
      **kwargs,
    )    

    if self.qurl is None:
      self.qurl = self._get_qurl(qurl)

    self.batch_size = AWS_BATCH_SIZE

  @retry
  def _get_qurl(self, qurl):
    try:
      return self.sqs.get_queue_url(QueueName=qurl)["QueueUrl"]
    except Exception as err:
      print(f"Failed to fetch queue URL for: {qurl}")
      raise    

  @property
  def enqueued(self):
    status = self.status()
    return int(status['ApproximateNumberOfMessages']) + int(status['ApproximateNumberOfMessagesNotVisible'])

  @property
  def inserted(self):
    return float('NaN')

  @property
  def completed(self):
    return float('NaN')

  @property
  def leased(self):
    status = self.status()
    return int(status['ApproximateNumberOfMessagesNotVisible'])

  def is_empty():
    return self.enqueued == 0

  @retry
  def status(self):
    resp = self.sqs.get_queue_attributes(
      QueueUrl=self.qurl, 
      AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    return resp['Attributes']

  @retry
  def insert(self, tasks, delay_seconds=0):
    tasks = toiter(tasks)

    total = 0
    # send_message_batch's max batch size is 10
    for batch in sip(tasks, self.batch_size):
      if len(batch) == 0:
        break

      entries = [ {
          "Id": str(j),
          "MessageBody": jsonify(task),
          "DelaySeconds": delay_seconds,
        } for j, task in enumerate(batch) 
      ]

      try:
        resp = self.sqs.send_message_batch(
          QueueUrl=self.qurl,
          Entries=entries,
        )
      except botocore.exceptions.ClientError as error:
        http_code = error.response['ResponseMetadata']['HTTPStatusCode']
        if 400 <= int(http_code) < 500:
          raise ClientSideError(error)
        else:
          raise error
      
      total += len(entries)

    return total

  def add_insert_count(self, ct):
    pass

  def rezero(self):
    pass

  def renew_lease(self, seconds):
    raise NotImplementedError() 

  def cancel_lease(self, rhandle):
    raise NotImplementedError()

  def release_all(self):
    raise NotImplementedError()

  def lease(self, seconds, num_tasks=1, wait_sec=20):
    if wait_sec is None:
      wait_sec = 20

    resp = self.sqs.receive_message(
      QueueUrl=self.qurl,
      AttributeNames=[
        'SentTimestamp'
      ],
      MaxNumberOfMessages=num_tasks,
      MessageAttributeNames=[
        'All'
      ],
      VisibilityTimeout=seconds,
      WaitTimeSeconds=wait_sec,
    )
        
    if 'Messages' not in resp:
      return []

    tasks = []
    for msg in resp['Messages']:
      task = json.loads(msg['Body'])
      task['id'] = msg['ReceiptHandle']
      tasks.append(task)
    return tasks

  def delete(self, task):
    if type(task) == str:
      rhandle = task
    else:
      try:
        rhandle = task._id
      except AttributeError:
        rhandle = task['id']

    try:
      self.sqs.delete_message(
        QueueUrl=self.qurl,
        ReceiptHandle=rhandle,
      )
    except botocore.exceptions.ClientError as err:
      pass

    return 1

  def tally(self):
    pass

  def purge(self, native=False):
    # can throw:
    # except botocore.errorfactory.PurgeQueueInProgress:
    if native:
      self.sqs.purge_queue(QueueUrl=self.qurl)
      return

    while self.enqueued:
      # visibility_timeout must be > 0 for delete to work
      tasks = self.lease(num_tasks=10, seconds=10)
      for task in tasks:
        self.delete(task)
    
  def __iter__(self):
    return iter(self.lease(num_tasks=10, seconds=0))
      





