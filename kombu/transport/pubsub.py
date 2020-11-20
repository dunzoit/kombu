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

    def exchange_declare(self, *args, **kwargs):
        return self.publisher.create_topic(
            request=kwargs.get("request"),
            topic=kwargs.get("topic"),
            timeout=kwargs.get("timeout"),
            metadata=kwargs.get("metadata"))

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
        return self.publisher.publish(
            exchange,
            message,
            ordering_key=routing_key,
            mandatory=mandatory, 
            immediate=immediate,
            **kwargs)

    def exchange_delete(self, *args, **kwargs):
        return self.publisher.delete_topic(
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

    def basic_consume(self, *args, **kwargs):
        return self.subscriber.subscribe(
            subscription=kwargs.get("subscription"),
            callback=kwargs.get("callback"),
            flow_control=kwargs.get("flow_control"),
            scheduler=kwargs.get("scheduler"))

    def basic_cancel(self, *args, **kwargs):
        self._called('basic_cancel')

    def basic_ack(self, *args, **kwargs):
        self.subscriber.acknowledge(
            request=kwargs.get("request"),
            subscription=kwargs.get("subscription"),
            ack_ids=kwargs.get("ack_ids"),
            timeout=kwargs.get("timeout"),
            metadata=kwargs.get("metadata"))

    def basic_recover(self, requeue=False):
        self._called('basic_recover')

    def exchange_bind(self, *args, **kwargs):
        self._called('exchange_bind')

    def exchange_unbind(self, *args, **kwargs):
        self._called('exchange_unbind')

    def close(self):
        self.subscriber.close()

    def message_to_python(self, message, *args, **kwargs):
        self._called('message_to_python')
        return Message(self, body=anyjson.dumps(message),
                       delivery_tag=next(self.deliveries),
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


class Connection(object):
    connected = True

    def establish_pubsub_connection(self, service_file=None):
        if service_file is None:
            raise NotImplementedError("service_file json required")
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
        credentials = jwt.Credentials.from_service_account_file(
            service_file, audience=audience)
        self.set_subscriber(credentials=credentials)
        self.set_publisher(credentials=credentials)

    def set_publisher(self, credentials):
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials_pub = credentials.with_claims(audience=audience)
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)

    def set_subscriber(self, credentials):
        self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    def __init__(self, client, **kwargs):
        self.establish_pubsub_connection(
            service_account_info=kwargs.get('service_account_info'))

    def channel(self):
        return Channel(self)


class Transport(base.Transport):
    def __init__(self, *args, **kwargs):
        super(Transport, self).__init__(*args, **kwargs)
        self.establish_connection(**kwargs)

    def establish_connection(self, **kwargs):
        return Connection(self.client, **kwargs)

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return 'event'

    def close_connection(self, connection):
        connection.connected = False
