from __future__ import absolute_import
import os
import logging
from amqp.protocol import queue_declare_ok_t
from kombu.exceptions import ChannelError
from kombu.five import Empty
from kombu.transport import virtual, base
from kombu.utils import cached_property, uuid
from kombu.utils.compat import OrderedDict
from anyjson import loads, dumps
try:
    from google.cloud import pubsub_v1
    from google.auth import jwt
    from google.oauth2 import service_account
    from google.api_core.exceptions import AlreadyExists
except:
    pubsub_v1 = None
LOG = logging.getLogger(__name__)


class InitializationError(Exception):
    pass


class Message(base.Message):
    ''' Message class '''

    def __init__(self, channel, msg, **kwargs):
        super(Message, self).__init__(
            channel,
            body=msg.message.data,
            delivery_tag=msg.message.message_id,
            **kwargs)

    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.
        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.
        """
        self.channel.basic_ack(self.delivery_tag)


class QoS(virtual.QoS):
    def __init__(self, channel):
        super(QoS, self).__init__(channel, 1)
        self._channel = channel
        self._not_yet_acked = OrderedDict()
    # def can_consume(self):
    #     '''
    #        Returns True if :class:`Channel` can consumer, False otherwise
    #        :rtype: bool
    #     '''
    #     return not self.prefetch_count or len(self._not_yet_acked) <\
    #         self.prefetch_count
    # def can_consume_max_estimate(self):
    #     if self.prefetch_count:
    #         return self.prefetch_count - len(self._not_yet_acked)
    #     return 1

    def append(self, delivery_tag, message):
        self._not_yet_acked[delivery_tag] = message

    def ack(self, delivery_tag):
        message, subscription_path = self._not_yet_acked.pop(delivery_tag)
        # with self._channel.subscriber:
        #     message.ack()
        self._channel.subscriber.\
            acknowledge(subscription_path, message.ack_id)

    def reject(self):
        pass


class Channel(virtual.Channel):
    QoS = QoS
    Message = Message
    connected = True
    cleint = None
    DONT_CARE = None

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self._publisher = None
        self._subscriber = None
        self._credentials = None
        self._queue_cache = {}

    def _size(self, queue):
        pass

    def _new_queue(self, queue, **kwargs):
        ''' Create a new subscription in gcp '''
        try:
            return self._queue_cache[queue]
        except KeyError:
            subscription_path =\
                self.subscriber.subscription_path(
                    self.project_id, queue)  # pylint: disable=no-member
            self._queue_cache[queue] = subscription_path
            return subscription_path

    def _get(self, queue):
        ''' get messages from the subscription '''
        subscription_path = self._new_queue(queue)
        resp = self.subscriber.pull(
            subscription_path, 1)  # pylint: disable=no-member
        if resp.received_messages:
            msgs = resp.received_messages
            print "MESSAGE:", msgs
            self.qos.append(msgs[0].message.message_id,
                            (msgs[0], subscription_path))
            return msgs[0]
        return Empty()
    # def _messages_to_python(self, msgs, subscription_path):
    #     self.qos.append(msgs[0].message.message_id, (msgs[0], subscription_path))
    #     return msgs[0]

    def queue_declare(self, queue=None, passive=False, *args, **kwargs):
        print "queue-declare", args, kwargs
        queue = queue or 'gcp.gen-%s' % uuid()
        # TODO: need to check case when passive is True
        if passive:
            raise ChannelError(
                'NOT FOUND - no queue {} in host {}'.format(
                    queue, self.connection.client.virtual_host or '/'),
                (50, 10), 'Channel.queue_declare', '404',
            )
        else:
            self._new_queue(queue, **kwargs)
        return queue_declare_ok_t(queue, self._size(queue), 0)

    def queue_bind(self, *args, **kwargs):
        print "queue-bind", args, kwargs
        subscription_path = self._new_queue(kwargs.get('queue'))
        topic_path = self.state.exchanges[kwargs.get('exchange')]
        try:
            self.subscriber.create_subscription(  # pylint: disable=no-member
                subscription_path, topic_path
            )
        except AlreadyExists:
            pass

    def exchange_declare(self, exchange='', **kwargs):
        print 'called exchange declare', exchange
        to_add = False
        if exchange not in self.state.exchanges:
            topic_path =\
                self.publisher.topic_path(
                    self.project_id, exchange)  # pylint: disable=no-member
            try:
                self.publisher.create_topic(
                    topic_path)  # pylint: disable=no-member
                to_add = True
            except AlreadyExists:
                to_add = True
            except Exception:
                raise ChannelError(
                    'NOT_FOUND - no exchange {0!r} in vhost {1!r}'.format(
                        exchange, self.connection.client.virtual_host or '/'),
                    (50, 10), 'Channel.exchange_declare', '404',
                )
            finally:
                if to_add:
                    self.state.exchanges[exchange] = topic_path

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False, **kwargs):
        topic_path =\
            self.publisher.topic_path(
                self.project_id, exchange)  # pylint: disable=no-member
        message = dumps(message).encode('utf-8')
        future = self.publisher.publish(
            topic_path, message, **kwargs)
        return future.result()

    @property
    def publisher(self):
        if self._publisher is None:
            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    @property
    def subscriber(self):
        if self._subscriber is None:
            self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @property
    def project_id(self):
        if not self.transport_options.get('project_id', ''):
            return os.getenv("GCP_PROJECT_ID")
        else:
            return self.transport_options.get('project_id', '')


class Transport(virtual.Transport):
    Channel = Channel
    state = virtual.BrokerState()
    driver_type = 'gcp_pubsub'
    driver_name = 'pubsub_v1'

    def __init__(self, *args, **kwargs):
        if pubsub_v1 is None:
            raise ImportError("The pubsub_v1 library is not installed")
        super(Transport, self).__init__(*args, **kwargs)
