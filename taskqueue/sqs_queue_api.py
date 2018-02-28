import json
import re

import boto3
import botocore

from .secrets import aws_credentials

class AWSTaskQueueAPI(object):
    def __init__(self, qurl):
        """qurl looks like https://sqs.us-east-1.amazonaws.com/DIGITS/wms-pull-queue"""
        matches = re.search(r'sqs.([\w\d-]+).amazonaws', qurl)

        if matches is None:
            raise ValueError(str(qrl) + ' is not a valid SQS url.')
        region_name, = matches.groups()

        self._qurl = qurl
        self._sqs = boto3.client('sqs', 
            region_name=region_name, 
            aws_secret_access_key=aws_credentials['AWS_SECRET_ACCESS_KEY'],
            aws_access_key_id=aws_credentials['AWS_ACCESS_KEY_ID'],
        )    

    @property
    def enqueued(self):
        return int(self.status()['ApproximateNumberOfMessages'])

    def status(self):
        resp = self._sqs.get_queue_attributes(QueueUrl=self._qurl, AttributeNames=['ApproximateNumberOfMessages'])
        return resp['Attrbutes']

    def insert(self, task):
        resp = self._sqs.send_message(
            QueueUrl=self._qurl,
            DelaySeconds=0,
            MessageBody=json.dumps(task),
        )
        return resp['MessageId']

    def lease(self, seconds=600):
        resp = self._sqs.receive_message(
            QueueUrl=self._qurl,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=seconds,
            WaitTimeSeconds=0
        )
        msg = resp['Messages'][0]
        receipt_handle = msg['ReceiptHandle']
        return receipt_handle

    def delete(self, rhandle):
        self._sqs.delete_message(
            QueueUrl=self._qurl,
            ReceiptHandle=rhandle,
        )

    def purge(self):
        try:
            return self._sqs.purge_queue(QueueUrl=self._qurl)
        except botocore.errorfactory.PurgeQueueInProgress:
            return None

    def list(self):
        return []













class SQSTaskQueue():
    def __init__(self, qurl):
        # qurl looks like https://sqs.us-east-1.amazonaws.com/DIGITS/wms-pull-queue
        matches = re.search(r'sqs.([\w\d-]+).amazonaws', qurl)

        if matches is None:
            raise ValueError(str(qrl) + ' is not a valid SQS url.')

        region_name, = matches.groups()

        self._qurl = qurl
        self._sqs = boto3.client('sqs', 
            region_name=region_name, 
            aws_secret_access_key=aws_credentials['AWS_SECRET_ACCESS_KEY'],
            aws_secret_access_key_id=aws_credentials['AWS_ACCESS_KEY_ID'],
        )

        self._tasks = SQSTaskQueue.Tasks(self)
        self._taskqueues = SQSTaskQueue.TaskQueues(self)

    def tasks(self):
        return self._task

    def taskqueues(self):
        return self._taskqueues

    class Tasks():
        def __init__(self, sqs, qurl):
            self._sqs = sqs
            self._qurl = qurl

        def insert(self,  project, taskqueue, body):

            if 'groupByTag' not in body or not body['groupByTag']:
                body['tag'] = ''
            
            if 'groupByTag' in body: del body['groupByTag']
            if 'queueName' in body: del body['queueName']

            self._sqs.send_message(
                QueueUrl=self._qurl,
                MessageBody=json.dumps(body),
            )
            
            return ExecuteWrapper(r.json())

        def get(self, project, taskqueue, task):
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks/{}'.format(
                project, taskqueue, task
            )
            r = requests.get(url, params=json.dumps(body))
            return ExecuteWrapper(r.json())

        def delete(self, project, taskqueue, task):
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks/id/{}'.format(project, taskqueue, task)
            r = requests.delete(url)
            return ExecuteWrapper(None)

        def lease(self, project, taskqueue, numTasks=1, leaseSecs=1, groupByTag=False, tag=''):

            if groupByTag == False:
                tag = ''
            
            params = {
                'numTasks': numTasks, 
                'leaseSecs': leaseSecs, 
                'tag': tag,
            }
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks/lease'.format(
                project, taskqueue
            )
            r = requests.post(url, params=params)
            return ExecuteWrapper(r.json())

        def list(self, project, taskqueue):
            url = self._ae._queue_url + '/{}/taskqueue/{}/tasks'.format(
                project, taskqueue
            )
            r = requests.get(url)
            return ExecuteWrapper(r.json())

    class TaskQueues():
        def __init__(self, appengine):
            self._ae = appengine

        def get(self, project, taskqueue, getStats=False):
            url = self._ae._queue_url + '/{}/taskqueue/{}'.format(
                project, taskqueue
            )
            r = requests.get(url)
            return ExecuteWrapper(r.json())





