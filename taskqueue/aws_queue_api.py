import json
import re
import types

import boto3
import botocore

from .secrets import aws_credentials

def toiter(obj):
  if isinstance(obj, types.GeneratorType):
    return obj

  try:
    if type(obj) is dict:
      return iter([ obj ])
    return iter(obj)
  except TypeError:
    return iter([ obj ])

class AWSTaskQueueAPI(object):
  def __init__(self, qurl, region_name=None):
    """
    qurl: either a queue name (e.g. 'pull_queue') or a url
      like https://sqs.us-east-1.amazonaws.com/DIGITS/wms-pull-queue
    """
    matches = re.search(r'sqs.([\w\d-]+).amazonaws', qurl)

    if matches is not None:
      region_name, = matches.groups()
      self._qurl = qurl
    else:
      self._qurl = None

    credentials = aws_credentials()
   
    self._sqs = boto3.client('sqs', 
      region_name=region_name, 
      aws_secret_access_key=credentials['AWS_SECRET_ACCESS_KEY'],
      aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],
    )    

    if self._qurl is None:
      self._qurl = self._sqs.get_queue_url(QueueName=qurl)["QueueUrl"]

  @property
  def enqueued(self):
    status = self.status()
    return int(status['ApproximateNumberOfMessages']) + int(status['ApproximateNumberOfMessagesNotVisible'])

  def status(self):
    resp = self._sqs.get_queue_attributes(
      QueueUrl=self._qurl, 
      AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    return resp['Attributes']

  def insert(self, tasks, delay_seconds=0):
    tasks = toiter(tasks)

    AWS_BATCH_SIZE = 10 

    resps = []
    batch = []
    while True:
      del batch[:]
      try:
        for i in range(10):
          batch.append(next(tasks))
      except StopIteration:
        pass
      
      if len(batch) == 0:
        break

      resp = self._sqs.send_message_batch(
        QueueUrl=self._qurl,
        Entries=[ {
          "Id": str(j),
          "MessageBody": json.dumps(task),
          "DelaySeconds": delay_seconds,
        } for j, task in enumerate(batch) ],
      )
      resps.append(resp)

    return resps

  def renew_lease(self, seconds):
    raise NotImplementedError() 

  def cancel_lease(self, rhandle):
    raise NotImplementedError()

  def _request(self, num_tasks, visibility_timeout):
    resp = self._sqs.receive_message(
      QueueUrl=self._qurl,
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

  def lease(self, seconds, numTasks=1, groupByTag=False, tag=''):
    if numTasks > 1:
      raise ValueError("This library (not boto/SQS) only supports fetching one task at a time. Requested: {}.".format(numTasks))
    return self._request(numTasks, seconds)

  def acknowledge(self, task):
    return self.delete(task)

  def delete(self, task):
    if type(task) == str:
      rhandle = task
    else:
      try:
        rhandle = task._id
      except AttributeError:
        rhandle = task['id']

    try:
      self._sqs.delete_message(
        QueueUrl=self._qurl,
        ReceiptHandle=rhandle,
      )
    except botocore.exceptions.ClientError as err:
      pass

  def purge(self):
    # This is more efficient, but it kept freezing
    # try:
    #     self._sqs.purge_queue(QueueUrl=self._qurl)
    # except botocore.errorfactory.PurgeQueueInProgress:

    while self.enqueued:
      # visibility_timeout must be > 0 for delete to work
      tasks = self._request(num_tasks=10, visibility_timeout=10)
      for task in tasks:
        self.delete(task)
    return self

  def list(self):
    return self._request(num_tasks=10, visibility_timeout=0)
      
      





