import json
import re
import types

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

from .secrets import google_credentials


def toiter(obj):
  if isinstance(obj, types.GeneratorType):
    return obj

  try:
    if type(obj) is dict:
      return iter([obj])
    return iter(obj)
  except TypeError:
    return iter([obj])


class GooglePubSubAPI(object):
  def __init__(self, topic, subscription):
    matches = re.search(r"projects/([^/]+)/topics/([^/]+)", topic)
    project, _ = matches.groups()
    self.topic = topic
    self.subscription = subscription

    # matches = re.search(rf'projects/{project}/subscriptions/([^/]+)', subscription)
    # self.subscription, = matches.groups()

    project, credentials = google_credentials(project)

    publisher_audience = (
      "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    )
    subscriber_audience = (
      "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
    )

    credentials_pub = credentials.with_claims({"audience": publisher_audience})
    credentials_sub = credentials.with_claims({"audience": subscriber_audience})

    self.publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
    self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials_sub)

  @property
  def enqueued(self):
    status = self.status()
    return int(status["ApproximateNumberOfMessages"]) + int(status["ApproximateNumberOfMessagesNotVisible"])

  def status(self):
    raise NotImplementedError()

  def insert(self, tasks, delay_seconds=0):
    tasks = toiter(tasks)

    resps = []
    while True:
      try:
        task = next(tasks)
      except StopIteration:
        return resps

      body = str.encode(json.dumps(task))
      future = self.publisher.publish(self.topic, body)
      resps.append(future)

    return resps

  def renew_lease(self, seconds):
    raise NotImplementedError()

  def cancel_lease(self, rhandle):
    raise NotImplementedError()

  def _request(self, num_tasks, visibility_timeout):
    response = self.subscriber.pull(
      self.subscription, max_messages=num_tasks, return_immediately=True
    )

    tasks = []
    for msg in response.received_messages:
      task = json.loads(msg.message.data.decode())
      task["id"] = msg.ack_id
      tasks.append(task)

    return tasks

  def lease(self, seconds, numTasks=1, groupByTag=False, tag=""):
    if numTasks > 1:
      raise ValueError(
        "This library (not boto/SQS) only supports fetching one task at a time. Requested: {}.".format(numTasks)
      )
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
        rhandle = task["id"]

    try:
      self.subscriber.acknowledge(self.subscription, [rhandle])
    except GoogleAPICallError:
      pass

  def purge(self):
    raise NotImplementedError()

  def list(self):
    return self._request(num_tasks=10, visibility_timeout=0)

