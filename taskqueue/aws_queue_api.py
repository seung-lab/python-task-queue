import json
import re
import types

import boto3
import botocore.errorfactory

from .lib import toiter, sip
from .secrets import aws_credentials
from .secrets import (
  AWS_DEFAULT_REGION
)

AWS_BATCH_SIZE = 10 # send_message_batch's max batch size is 10

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
      try:
        self.qurl = self.sqs.get_queue_url(QueueName=qurl)["QueueUrl"]
      except Exception:
        print(qurl)
        raise

    self.batch_size = AWS_BATCH_SIZE

  @property
  def enqueued(self):
    status = self.status()
    return int(status['ApproximateNumberOfMessages']) + int(status['ApproximateNumberOfMessagesNotVisible'])

  @property
  def inserted(self):
    raise NotImplementedError()

  @property
  def completed(self):
    raise NotImplementedError()

  def is_empty():
    return self.enqueued == 0

  def status(self):
    resp = self.sqs.get_queue_attributes(
      QueueUrl=self.qurl, 
      AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    return resp['Attributes']

  def insert(self, tasks, delay_seconds=0):
    tasks = toiter(tasks)

    total = 0
    # send_message_batch's max batch size is 10
    for batch in sip(tasks, self.batch_size):
      if len(batch) == 0:
        break

      resp = self.sqs.send_message_batch(
        QueueUrl=self.qurl,
        Entries=[ {
          "Id": str(j),
          "MessageBody": json.dumps(task),
          "DelaySeconds": delay_seconds,
        } for j, task in enumerate(batch) ],
      )
      total += 1

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

  def _request(self, num_tasks, visibility_timeout):
    resp = self.sqs.receive_message(
      QueueUrl=self.qurl,
      AttributeNames=[
        'SentTimestamp'
      ],
      MaxNumberOfMessages=num_tasks,
      MessageAttributeNames=[
        'All'
      ],
      VisibilityTimeout=visibility_timeout,
      WaitTimeSeconds=20,
    )
        
    if 'Messages' not in resp:
      return []

    tasks = []
    for msg in resp['Messages']:
      task = json.loads(msg['Body'])
      task['id'] = msg['ReceiptHandle']
      tasks.append(task)
    return tasks

  def lease(self, seconds, num_tasks=1):
    if num_tasks > 1:
      raise ValueError("This library (not boto/SQS) only supports fetching one task at a time. Requested: {}.".format(num_tasks))
    return self._request(num_tasks, seconds)

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

  def tally(self):
    raise NotImplementedError("Not supported for SQS.")

  def purge(self):
    # This is more efficient, but it kept freezing
    # try:
    #     self.sqs.purge_queue(QueueUrl=self.qurl)
    # except botocore.errorfactory.PurgeQueueInProgress:

    while self.enqueued:
      # visibility_timeout must be > 0 for delete to work
      tasks = self._request(num_tasks=10, visibility_timeout=10)
      for task in tasks:
        self.delete(task)
    return self

  def list(self):
    return list(self)
    
  def __iter__(self):
    return iter(self._request(num_tasks=10, visibility_timeout=0))
      





