from __future__ import absolute_import
import os
from queue import Queue

from anyjson import loads, dumps
from amqp.protocol import queue_declare_ok_t

from kombu.exceptions import ChannelError
from kombu.five import Empty
from kombu.log import get_logger
from kombu.transport import virtual, base
from kombu.utils import cached_property, uuid
from kombu.utils.compat import OrderedDict

try:
    from google.cloud import pubsub_v1
    from google.auth import jwt
    from google.oauth2 import service_account
    from google.api_core.exceptions import AlreadyExists
except:
    pubsub_v1 = None

logger = get_logger(__name__)


class Message(base.Message):

    def __init__(self, channel, msg, **kwargs):
        super(Message, self).__init__(
            channel,
            body=msg.message.data,
            delivery_tag=msg.message.message_id,
            **kwargs)

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
        import pdb; pdb.set_trace()
        message, subscription_path = self._not_yet_acked.pop(delivery_tag)
        self._channel.subscriber.\
            acknowledge(subscription_path, message.ack_id)


class Channel(virtual.Channel):
    QoS = QoS
    Message = Message

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self._publisher = None
        self._subscriber = None
        self._credentials = None
        self._queue_cache = {}
        self.temp_cache = Queue(maxsize=self.max_messages)

    def _new_queue(self, queue, **kwargs):
        """Create a new subscription in gcp

        :param queue: queue name
        :type body: str

        :return: subscription_path
        :rtype: str

        """
        try:
            return self._queue_cache[queue]
        except KeyError:
            subscription_path =\
                self.subscriber.subscription_path(
                    self.project_id, queue)
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
        if not self.temp_cache.empty():
            return self.temp_cache.get()
        subscription_path = self._new_queue(queue)
        resp = self.subscriber.pull(
            subscription_path, self.max_messages)
        if resp.received_messages:
            for msg in resp.received_messages:
                if self.temp_cache.full():
                    break
                self.qos.append(msg.message.message_id,
                                (msg, subscription_path))
                self.temp_cache.put(msg)
            return self.temp_cache.get()
        return Empty()

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
        try:
            self.subscriber.create_subscription(
                subscription_path, topic_path,
                ack_deadline_seconds=self.ack_deadline_seconds)
        except AlreadyExists:
            pass

    def exchange_declare(self, exchange='', **kwargs):
        """Declare a topic in PubSub

        :param exchange: queue name
        :type body: str
        """
        to_add = False
        if exchange not in self.state.exchanges:
            topic_path = self.publisher.topic_path(self.project_id, exchange)
            try:
                self.publisher.create_topic(
                    topic_path)
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


    @property
    def publisher(self):
        """PubSub Publisher credentials"""
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    @property
    def subscriber(self):
        """PubSub Subscriber credentials"""
        if self._subscriber is None:
            self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    @property
    def transport_options(self):
        """PubSub Transport sepcific configurations"""
        return self.connection.client.transport_options

    @property
    def project_id(self):
        """GCP Project ID"""
        if not self.transport_options.get('PROJECT_ID', ''):
            return os.getenv("GCP_PROJECT_ID")
        return self.transport_options.get('PROJECT_ID', '')

    @property
    def max_messages(self):
        """Maximum messages to pull into local cache"""
        return self.transport_options.get('MAX_MESSAGES', 10)

    @property
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
