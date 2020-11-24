from __future__ import absolute_import

from itertools import count

import anyjson

from kombu.transport import base

import json

# gcp
from google.cloud import pubsub_v1
from google.auth import jwt


class Message(base.Message):

    def __init__(self, channel, msg, **kwargs):
        props = msg.properties
        super(Message, self).__init__(
            channel,
            body=msg.body,
            delivery_tag=msg.delivery_tag,
            content_type=props.get('content_type'),
            content_encoding=props.get('content_encoding'),
            delivery_info=msg.delivery_info,
            properties=msg.properties,
            headers=props.get('application_headers') or {},
            **kwargs)
    
    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.channel.no_ack_consumers is not None:
            try:
                consumer_tag = self.delivery_info['consumer_tag']
            except KeyError:
                pass
            else:
                if consumer_tag in self.channel.no_ack_consumers:
                    return
        if self.acknowledged:
            raise self.MessageStateError(
                'Message already acknowledged with state: {0._state}'.format(
                    self))
        self.channel.basic_ack(self.delivery_tag)
        self._state = 'ACK'


class Channel(base.StdChannel):
    open = True
    throw_decode_error = False
    _ids = count(1)

    def __init__(self, connection):
        self.connection = connection
        self.called = []
        self.deliveries = count(1)
        self.to_deliver = []
        self.events = {'basic_return': set()}
        self.channel_id = next(self._ids)

    def _called(self, name):
        self.called.append(name)

    def __contains__(self, key):
        return key in self.called

    def exchange_declare(self, exchange='', **kwargs):
        topic_path =\
            self.connection.publisher_client(exchange).\
            topic_path(self.connection.PROJECT, exchange)
        try:
            return self.connection.publisher_client(exchange).\
                create_topic(topic_path)
        except Exception as exc:
            raise exc

    def prepare_message(self, body, priority=0, content_type=None,
                        content_encoding=None, headers=None, properties={}):
        self._called('prepare_message')
        return dict(body=body,
                    headers=headers,
                    properties=properties,
                    priority=priority,
                    content_type=content_type,
                    content_encoding=content_encoding)

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False, **kwargs):
        return self.connection.publisher.publish(
            exchange,
            message,
            ordering_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            **kwargs)

    def exchange_delete(self, *args, **kwargs):
        return self.connection.publisher.delete_topic(
            request=kwargs.get("request"),
            topic=kwargs.get("topic"),
            timeout=kwargs.get("timeout"),
            metadata=kwargs.get("metadata"))

    def queue_declare(self, *args, **kwargs):
        self._called('queue_declare')

    def queue_bind(self, *args, **kwargs):
        self._called('queue_bind')

    def queue_unbind(self, *args, **kwargs):
        self._called('queue_unbind')

    def queue_delete(self, queue, if_unused=False, if_empty=False, **kwargs):
        self._called('queue_delete')

    def basic_get(self, *args, **kwargs):
        self._called('basic_get')
        try:
            return self.to_deliver.pop()
        except IndexError:
            pass

    def queue_purge(self, *args, **kwargs):
        self._called('queue_purge')

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Consume from `queue`"""
        self._tag_to_queue[consumer_tag] = queue
        self._active_queues.append(queue)

        def _callback(raw_message):
            message = self.Message(self, raw_message)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return callback(message)

        self.connection._callbacks[queue] = _callback
        return super(Channel, self).basic_consume(queue, no_ack, *args, **kwargs)

    def basic_cancel(self, *args, **kwargs):
        self._called('basic_cancel')

    def basic_ack(self, delivery_tag):
        self.connection.subscriber.acknowledge(
            request=kwargs.get("request"),
            subscription=kwargs.get("subscription"),
            ack_ids=delivery_tag,
            timeout=kwargs.get("timeout"),
            metadata=kwargs.get("metadata"))
        super(Channel, self).basic_ack(delivery_tag)

    def basic_recover(self, requeue=False):
        self._called('basic_recover')

    def exchange_bind(self, *args, **kwargs):
        self._called('exchange_bind')

    def exchange_unbind(self, *args, **kwargs):
        self._called('exchange_unbind')

    def close(self):
        self.connection.subscriber.close()

    def message_to_python(self, message, *args, **kwargs):
        self._called('message_to_python')
        return Message(self, body=msg.data,
                       delivery_tag=msg.ack_id,
                       throw_decode_error=self.throw_decode_error,
                       content_type='application/json',
                       content_encoding='utf-8')

    def flow(self, active):
        self._called('flow')

    def basic_reject(self, delivery_tag, requeue=False):
        if requeue:
            return self._called('basic_reject:requeue')
        return self._called('basic_reject')

    def basic_qos(self, prefetch_size=0, prefetch_count=0,
                  apply_global=False):
        self._called('basic_qos')
    
    def message_generator(self, q):
        while self.connection.connected:
            response = self.subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": 5,
                }
            )
            for msg in response.received_messages:
                q.put(msg.message)

            ack_ids = [msg.ack_id for msg in response.received_messages]
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids,
                }
            )
            


class Connection(object):
    connected = True
    client = None
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.{}"
    PROJECT = None
    SUBSCRIPTION = None

    def __init__(self, client, options=None):
        self.client = client
        self.options = options
        self.PROJECT = options.get("project_id")
        self.SUBSCRIPTION = options.get("topic_name")
        self._credentials = None
        self._conn = None
        self._publishers = {}
        self._subscribers = {}
        self._callbacks = {}

    def publisher_client(self, queue):
        try:
            return self._publishers[queue]
        except KeyError:
            audience = self.audience.format("Publisher")
            publisher = pubsub_v1.PublisherClient(
                credentials=self.credentials(audience))
            self._publishers[queue] = publisher
            return publisher

    def credentials(self, audience):
        if self._credentials is None:
            self._credentials = jwt.Credentials.from_service_account_file(
                self.options.get("service_file"), audience=audience)
        return self._credentials

    def subscriber_client(self, queue):
        try:
            return self._subscribers[queue]
        except KeyError:
            audience = self.audience.format("Subscriber")
            subscriber = pubsub_v1.SubscriberClient(
                credentials=self.credentials(audience))
            self._subscribers[queue] = subscriber
            return subscriber
    
    def register_with_event_loop(self, connection, loop):
        self.r, self._w = os.pipe()
        if fcntl is not None:
            fcntl.fcntl(self.r, fcntl.F_SETFL, os.O_NONBLOCK)
        self.use_async_interface = True
        loop.add_reader(self.r, self.on_readable, connection, loop)


    def establish_pubsub_connection(self, service_info=None):
        if service_info is None:
            raise NotImplementedError("service_info json required")
        audience = self.audience.format("Subscriber")
        credentials = jwt.Credentials.from_service_account_file(
            self.options.get("service_file"), audience=audience)
        self.set_subscriber(credentials=credentials)
        self.set_publisher(credentials=credentials)

    def set_publisher(self, credentials):
        audience = self.audience.format("Publisher")
        credentials_pub = credentials.with_claims(audience=audience)
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)

    def set_subscriber(self, credentials):
        self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    def channel(self):
        return Channel(self)
    
    def drain_events(self, connection, **kwargs):
        response = self.subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 1,
            }
        )
        for msg in response.received_messages:
            return msg

        ack_ids = [msg.ack_id for msg in response.received_messages]
        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
            }
        )
        return msg
        


class Transport(base.Transport):
    options = None

    def __init__(self, client,
                 default_port=None, default_ssl_port=None, **kwargs):
        self.client = client
        self.options = self.client.transport_options

    def establish_connection(self, **kwargs):
        return Connection(self.client, options= self.options)
    
    def register_with_event_loop(self, connection, loop):
        loop.add_reader(connection.method_reader.source.sock,
                        self.on_readable, connection, loop)

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return connection.drain_events(**kwargs)

    def close_connection(self, connection):
        connection.connected = False
    
    def on_readable(self, connection, loop):
        os.read(self.r, 1)
        try:
            self.drain_events(self.connection)
        except socket.timeout:
            pass
