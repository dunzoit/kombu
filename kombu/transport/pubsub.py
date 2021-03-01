from __future__ import absolute_import

import os
import sys
from dateutil import parser
from threading import Thread

from anyjson import dumps, loads
from amqp.protocol import queue_declare_ok_t

from kombu.exceptions import ChannelError
from kombu.five import Empty, Queue
from kombu.log import get_logger
from kombu.transport import virtual, base
from kombu.utils import cached_property, uuid
from kombu.utils.compat import OrderedDict

import google.auth
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from google.api_core.exceptions import AlreadyExists, DeadlineExceeded

logger = get_logger(__name__)


class Worker(Thread):
    ''' Worker thread '''
    def __init__(
            self, client, subscription_path, max_messages,
            queue, return_immediately):
        Thread.__init__(self)
        self.subscriber = client
        self.subscription_path = subscription_path
        self.queue = queue
        self.max_messages = max_messages
        self.return_immediately = return_immediately
        self.start()

    def callback(self, msg):
        self.queue.put(msg, block=True)

    def run(self):
        ''' run '''
        while True:
            logger.info("".join(["Pulling messsage using subscription ",
                self.subscription_path]))
            try:
                resp =\
                    self.subscriber.pull(
                        self.subscription_path, self.max_messages,
                        return_immediately=self.return_immediately)
            except (ValueError, DeadlineExceeded):
                continue
            if resp.received_messages:
                for msg in resp.received_messages:
                    self.queue.put(msg, block=True)


class Message(base.Message):
    def __init__(self, channel, msg, **kwargs):
        body, props = self._translate_message(msg)
        super(Message, self).__init__(
            channel,
            body=body,
            delivery_tag=msg.message.message_id,
            content_type=props.get('content_type'),
            content_encoding=props.get('content_encoding'),
            delivery_info=props.get('delivery_info'),
            properties=props,
            headers=props.get('headers') or {},
            **kwargs)

    def _translate_message(self, raw_message):
        serialized = loads(raw_message.message.data)
        properties = {
            'headers': serialized['headers'],
            'content_type': serialized['content-type'],
            'reply_to': serialized['properties']['reply_to'],
            'correlation_id': serialized['properties']['correlation_id'],
            'delivery_mode': serialized['properties']['delivery_mode'],
            'delivery_info': serialized['properties']['delivery_info'],
            'content_encoding': serialized['content-encoding']
        }
        return serialized['body'], properties

    def ack(self):
        """"Send an acknowledgement of the message consumed
        """
        self.channel.basic_ack(self.delivery_tag)


class QoS(virtual.QoS):
    def __init__(self, channel):
        super(QoS, self).__init__(channel, 1)
        self._channel = channel
        self._not_yet_acked = OrderedDict()

    def append(self, delivery_tag, message):
        """Append message to consume
        :param delivery_tag: delivery tag for message
        :type body: str
        :keyword message: The message received from the queue.
        :type encoding: str
        """
        self._not_yet_acked[delivery_tag] = message

    def ack(self, delivery_tag):
        """Send an acknowledgement of the message consumed
        :param delivery_tag: delivery tag for message
        :type body: str
        """
        message, subscription_path = self._not_yet_acked.pop(delivery_tag)
        self._channel.subscriber.\
            acknowledge(subscription_path, [message.ack_id])


class Channel(virtual.Channel):
    QoS = QoS
    Message = Message

    TOPIC_PATH = "projects/{}/topics/{}"
    SUBSCRIPTION_NAME = "projects/{}/subscriptions/{}"

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self._queue_cache = {}
        self.temp_cache = {}

    def _get_topic_path(self, exchange):
        return self.TOPIC_PATH.format(self.project_id, exchange)

    def _get_subscription_name(self, subscription):
        return self.SUBSCRIPTION_NAME.format(self.project_id, subscription)

    def _new_queue(self, queue, **kwargs):
        """Create a new subscription in gcp
        :param queue: queue name
        :type body: str
        :return: subscription_path
        :rtype: str
        """
        if 'pid' in queue:
            queue = queue.replace("@", ".")
        try:
            return self._queue_cache[queue]
        except KeyError:
            subscription_path =\
                self._get_subscription_name(queue)
            self._queue_cache[queue] = subscription_path
            return subscription_path

    def _get(self, queue):
        """Get a message from queue
        The message is pulled from PubSub. 
        To boost the consumption of messages the cache size 
        might be adjusted to pull multiple messages at once 
        by adjusting MAX_MESSAGES.
        :param queue: queue name
        :type body: str
        :return: message
        :rtype: Message
        """
        if queue in self.ignored_queues:
            raise Empty()
        subscription_path = self._new_queue(queue)
        return getattr(self,
            self._execution_type() + "_msg_get")(subscription_path)

    def _concurrent_msg_get(self, subscription_path):
        if not self.temp_cache[subscription_path].empty():
            msg = self.temp_cache[subscription_path].get(block=True)
            self.qos.append(
                msg.message.message_id, (msg, subscription_path))
            return msg
        raise Empty()

    def _msg_get(self, subscription_path):
        if not self.temp_cache[subscription_path].empty():
            return self.temp_cache[subscription_path].get(block=True)
        logger.info("".join([
            "Pulling messsage using subscription ", subscription_path]))
        resp = self.subscriber.pull(
                subscription_path, self.max_messages,
                return_immediately=self.return_immediately)
        if resp.received_messages:
            for msg in resp.received_messages:
                if self.temp_cache[subscription_path].full():
                    break
                self.qos.append(msg.message.message_id,
                                (msg, subscription_path))
                self.temp_cache[subscription_path].put(msg)
            return self.temp_cache[subscription_path].get(block=True)
        raise Empty()

    def queue_declare(self, queue=None, passive=False, *args, **kwargs):
        """Create a new subscription
        :param queue: queue name
        :type body: str
        :return: message
        :rtype: Message
        """
        queue = queue or 'gcp.gen-%s' % uuid()
        # TODO: need to check case when passive is True
        if passive:
            raise ChannelError(
                'NOT FOUND - no queue {} in host {}'.format(
                    queue, self.connection.client.virtual_host or '/'),
                (50, 10), 'Channel.queue_declare', '404')
        self._new_queue(queue, **kwargs)
        return queue_declare_ok_t(queue, self._size(queue), 0)

    def queue_bind(self, *args, **kwargs):
        """Bind to a subscription
        :param queue: queue name
        :type body: str
        :param exchange: exchange name
        :type body: str
        """
        subscription_path = self._new_queue(kwargs.get('queue'))
        # topic_path = self.state.exchanges[kwargs.get('exchange')]
        # try:
        #     self.subscriber.create_subscription(
        #         subscription_path, topic_path,
        #         ack_deadline_seconds=self.ack_deadline_seconds)
        #     logger.info("".join(["Created subscription: ", subscription_path]))
        # except AlreadyExists:
        #     logger.info("".join(["Subscription already exists: ", subscription_path]))
        #     pass

        queue = Queue(maxsize=self.max_messages)
        self.temp_cache[subscription_path] = queue

        # if concurrent executions then start worker threads
        if self._execution_type() == "_concurrent":
            if kwargs.get('queue') in self.ignored_queues:
                return
            logger.info("".join([
                "Starting worker: ", subscription_path,
                " with queue size: ", str(self.max_messages)]))
            Worker(
                self.subscriber, subscription_path,
                self.max_messages, queue, self.return_immediately)

    def exchange_declare(self, exchange='', **kwargs):
        """Declare a topic in PubSub
        :param exchange: queue name
        :type body: str
        """
        to_add = False
        if exchange not in self.state.exchanges:
            logger.info("".join(["Topic: ", exchange, " not found added in state"]))
            topic_path = self._get_topic_path(exchange)
            self.state.exchanges[exchange] = topic_path
            # try:
            #     logger.info("Creating new topic: " + exchange)
            #     self.publisher.create_topic(topic_path)
            #     to_add = True
            # except AlreadyExists:
            #     to_add = True
            # except Exception as e:
            #     raise ChannelError(
            #         '{0} - no exchange {1!r} in vhost {2!r}'.format(
            #             e.__str__(),
            #             exchange,
            #             self.connection.client.virtual_host or '/'),
            #         (50, 10), 'Channel.exchange_declare', '404',
            #     )
            # finally:
            #     logger.info("".join(["adding topic: ", exchange, " to state"]))
            #     if to_add:
            #         self.state.exchanges[exchange] = topic_path

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False, **kwargs):
        """Publish message to PubSub
        :param message: message to publish
        :type body: str
        :param exchange: topic name
        :type body: str
        """
        if loads(message['body'])['eta']:
            return self._create_cloud_task(exchange, message)
        return self._publish(exchange, message, **kwargs)

    def _publish(self, topic, message, **kwargs):
        ''' publish the message '''
        topic_path =\
            self.publisher.topic_path(
                self.project_id, topic)
        message = dumps(message).encode('utf-8')
        future = self.publisher.publish(
            topic_path, message, **kwargs)
        return future.result()   

    def _create_cloud_task(self, exchange, message):
        ''' send task to cloud task '''
        eta = loads(message['body'])['eta']
        task = self._get_task(eta, exchange, message)
        return self.cloud_task.create_task(self.cloud_task_queue_path, task)

    def _get_task(self, eta, exchange, message):
        parsed_time = parser.parse(eta.strip())
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(parsed_time)
        return {
            "http_request": {
                "http_method": tasks_v2.enums.HttpMethod.POST,
                "oidc_token": {
                    "service_account_email": self.service_account_email,
                },
                "headers": {"Content-type": "application/json"},
                "url": self.transport_options.get("CLOUD_FUNCTION_PUBLISHER"),
                "body": dumps({
                    'destination_topic': exchange,
                    'eta': eta,
                    'message': message
                }).encode('utf-8'),
            },
            "name": self.cloud_task_queue_path + "/tasks/" + "_".join(
                    [exchange, uuid()]),
            "schedule_time": ts,
        }

    def _execution_type(self):
        if self.transport_options.get("CONCURRENT_PULLS", True):
            return '_concurrent'
        return ''

    @cached_property
    def publisher(self):
        """PubSub Publisher credentials"""
        return pubsub_v1.PublisherClient()

    @cached_property
    def subscriber(self):
        """PubSub Subscriber credentials"""
        return pubsub_v1.SubscriberClient()

    @cached_property
    def cloud_task(self):
        """ Client connection for cloud task """
        return tasks_v2.CloudTasksClient()

    @cached_property
    def return_immediately(self):
        """ return immediately from pull request """
        return self.transport_options.get("RETURN_IMMEDIATELY", True)

    @cached_property
    def service_account_email(self):
        email = self.transport_options.get("SERVICE_ACCOUNT_EMAIL", None)
        if email:
            return email
        creds, _ = google.auth.default()
        return creds.service_account_email

    @cached_property
    def transport_options(self):
        """PubSub Transport sepcific configurations"""
        return self.connection.client.transport_options

    @cached_property
    def project_id(self):
        """GCP Project ID"""
        if not self.transport_options.get('PROJECT_ID', ''):
            return os.getenv("GCP_PROJECT_ID")
        return self.transport_options.get('PROJECT_ID', '')

    @cached_property
    def max_messages(self):
        """Maximum messages to pull into local cache"""
        return self.transport_options.get('MAX_MESSAGES', 10)

    @cached_property
    def ack_deadline_seconds(self):
        """Deadline for acknowledgement from the time received.
        This is notified to PubSub while subscribing from the client.
        """
        return self.transport_options.get('ACK_DEADLINE_SECONDS', 60)

    @cached_property
    def cloud_task_queue_path(self):
        """ Cloud task queue path """
        return self.cloud_task.queue_path(
            self.project_id, self.location, self.delayed_queue)

    @cached_property
    def location(self):
        """ Cloud task queue location """
        return self.transport_options.get('QUEUE_LOCATION', None)

    @cached_property
    def delayed_queue(self):
        """Delayed topic used to support delay messages in celery"""
        return self.transport_options.get('DELAYED_QUEUE', None)

    @cached_property
    def ignored_queues(self):
        """ Queues to ignore """
        return self.transport_options.get('IGNORED_QUEUES', [])


class Transport(virtual.Transport):
    Channel = Channel
    state = virtual.BrokerState()
    driver_type = 'gcp_pubsub'
    driver_name = 'pubsub_v1'

    def __init__(self, *args, **kwargs):
        super(Transport, self).__init__(*args, **kwargs)
