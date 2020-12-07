from __future__ import absolute_import

import os
import sys

from anyjson import dumps, loads
from amqp.protocol import queue_declare_ok_t

from kombu.exceptions import ChannelError
from kombu.five import Empty, Queue
from kombu.log import get_logger
from kombu.transport import virtual, base
from kombu.utils import cached_property, uuid
from kombu.utils.compat import OrderedDict

try:
    from google.cloud import pubsub_v1
    from google.api_core.exceptions import AlreadyExists
except:
    pubsub_v1 = None

logger = get_logger(__name__)


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
        if 'celery' in queue:
            raise Empty()
        subscription_path = self._new_queue(queue)
        if not self.temp_cache[subscription_path].empty():
            return self.temp_cache[subscription_path].get(block=True)
        logger.info("".join(["Pulling messsage using subscription ", subscription_path]))
        resp = self.subscriber.\
            pull(subscription_path, self.max_messages, return_immediately=True)
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
        topic_path = self.state.exchanges[kwargs.get('exchange')]
        self.temp_cache[subscription_path] =\
            Queue(maxsize=self.max_messages)
        try:
            self.subscriber.create_subscription(
                subscription_path, topic_path,
                ack_deadline_seconds=self.ack_deadline_seconds)
            logger.info("".join(["Created subscription: ", subscription_path]))
        except AlreadyExists:
            logger.info("".join(["Subscription already exists: ", subscription_path]))
            pass

    def exchange_declare(self, exchange='', **kwargs):
        """Declare a topic in PubSub
        :param exchange: queue name
        :type body: str
        """
        to_add = False
        if exchange not in self.state.exchanges:
            logger.info("".join(["Topic: ", exchange, " not found added in state"]))
            topic_path = self._get_topic_path(exchange)
            try:
                logger.info("Creating new topic: " + exchange)
                self.publisher.create_topic(topic_path)
                to_add = True
            except AlreadyExists:
                to_add = True
            except Exception as e:
                raise ChannelError(
                    '{0} - no exchange {1!r} in vhost {2!r}'.format(
                        e.__str__(),
                        exchange,
                        self.connection.client.virtual_host or '/'),
                    (50, 10), 'Channel.exchange_declare', '404',
                )
            finally:
                logger.info("".join(["adding topic: ", exchange, " to state"]))
                if to_add:
                    self.state.exchanges[exchange] = topic_path

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False, **kwargs):
        """Publish message to PubSub
        :param message: message to publish
        :type body: str
        :param exchange: topic name
        :type body: str
        """
        topic_path =\
            self.publisher.topic_path(
                self.project_id, exchange)
        message = dumps(message).encode('utf-8')
        future = self.publisher.publish(
            topic_path, message, **kwargs)
        return future.result()

    @cached_property
    def publisher(self):
        """PubSub Publisher credentials"""
        return pubsub_v1.PublisherClient()

    @cached_property
    def subscriber(self):
        """PubSub Subscriber credentials"""
        return pubsub_v1.SubscriberClient()

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


class Transport(virtual.Transport):
    Channel = Channel
    state = virtual.BrokerState()
    driver_type = 'gcp_pubsub'
    driver_name = 'pubsub_v1'

    def __init__(self, *args, **kwargs):
        if pubsub_v1 is None:
            raise ImportError("The pubsub_v1 library is not installed")
        super(Transport, self).__init__(*args, **kwargs)
