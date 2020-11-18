''' module: pubsub '''
''' module: pubsub '''

"""This application demonstrates how to perform basic operations on topics
with the Cloud Pub/Sub API.
For more information, see the README.md under /pubsub and the documentation
at https://cloud.google.com/pubsub/docs.
"""

import argparse
from google.cloud import pubsub_v1
from . import virtual

class Functions:

    def list_topics(self, project_id):
        """Lists all Pub/Sub topics in the given project."""
        # [START pubsub_list_topics]

        # TODO(developer)
        # project_id = "your-project-id"

        publisher = pubsub_v1.PublisherClient()
        project_path = f"projects/{project_id}"

        for topic in publisher.list_topics(request={"project": project_path}):
            print(topic)
        # [END pubsub_list_topics]


    def create_topic(self, project_id, topic_id):
        """Create a new Pub/Sub topic."""
        # [START pubsub_quickstart_create_topic]
        # [START pubsub_create_topic]

        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        topic = publisher.create_topic(request={"name": topic_path})

        print("Created topic: {}".format(topic.name))
        # [END pubsub_quickstart_create_topic]
        # [END pubsub_create_topic]


    def delete_topic(project_id, topic_id):
        """Deletes an existing Pub/Sub topic."""
        # [START pubsub_delete_topic]

        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        publisher.delete_topic(request={"topic": topic_path})

        print("Topic deleted: {}".format(topic_path))
        # [END pubsub_delete_topic]


    def publish_messages(project_id, topic_id):
        """Publishes multiple messages to a Pub/Sub topic."""
        # [START pubsub_quickstart_publisher]
        # [START pubsub_publish]

        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)

        for n in range(1, 10):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data)
            print(future.result())

        print(f"Published messages to {topic_path}.")
        # [END pubsub_quickstart_publisher]
        # [END pubsub_publish]


    def publish_messages_with_custom_attributes(project_id, topic_id):
        """Publishes multiple messages with custom attributes
        to a Pub/Sub topic."""
        # [START pubsub_publish_custom_attributes]

        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        for n in range(1, 10):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            # Add two attributes, origin and username, to the message
            future = publisher.publish(
                topic_path, data, origin="python-sample", username="gcp"
            )
            print(future.result())

        print(f"Published messages with custom attributes to {topic_path}.")
        # [END pubsub_publish_custom_attributes]


    def publish_messages_with_error_handler(project_id, topic_id):
        # [START pubsub_publish_with_error_handler]
        """Publishes multiple messages to a Pub/Sub topic with an error handler."""
        import time


        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        futures = dict()

        def get_callback(f, data):
            def callback(f):
                try:
                    print(f.result())
                    futures.pop(data)
                except:  # noqa
                    print("Please handle {} for {}.".format(f.exception(), data))

            return callback

        for i in range(10):
            data = str(i)
            futures.update({data: None})
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data.encode("utf-8"))
            futures[data] = future
            # Publish failures shall be handled in the callback function.
            future.add_done_callback(get_callback(future, data))

        # Wait for all the publish futures to resolve before exiting.
        while futures:
            time.sleep(5)

        print(f"Published messages with error handler to {topic_path}.")
        # [END pubsub_publish_with_error_handler]


    def publish_messages_with_batch_settings(project_id, topic_id):
        """Publishes multiple messages to a Pub/Sub topic with batch settings."""
        # [START pubsub_publisher_batch_settings]
        from google.cloud import pubsub_v1

        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        # Configure the batch to publish as soon as there is ten messages,
        # one kilobyte of data, or one second has passed.
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=10,  # default 100
            max_bytes=1024,  # default 1 MB
            max_latency=1,  # default 10 ms
        )
        publisher = pubsub_v1.PublisherClient(batch_settings)
        topic_path = publisher.topic_path(project_id, topic_id)

        # Resolve the publish future in a separate thread.
        def callback(future):
            message_id = future.result()
            print(message_id)

        for n in range(1, 10):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            future = publisher.publish(topic_path, data)
            # Non-blocking. Allow the publisher client to batch multiple messages.
            future.add_done_callback(callback)

        print(f"Published messages with batch settings to {topic_path}.")
        # [END pubsub_publisher_batch_settings]


    def publish_messages_with_retry_settings(project_id, topic_id):
        """Publishes messages with custom retry settings."""
        # [START pubsub_publisher_retry_settings]
        from google import api_core
        from google.cloud import pubsub_v1

        # TODO(developer)
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        # Configure the retry settings. Defaults shown in comments are values applied
        # by the library by default, instead of default values in the Retry object.
        custom_retry = api_core.retry.Retry(
            initial=0.250,  # seconds (default: 0.1)
            maximum=90.0,  # seconds (default: 60.0)
            multiplier=1.45,  # default: 1.3
            deadline=300.0,  # seconds (default: 60.0)
            predicate=api_core.retry.if_exception_type(
                api_core.exceptions.Aborted,
                api_core.exceptions.DeadlineExceeded,
                api_core.exceptions.InternalServerError,
                api_core.exceptions.ResourceExhausted,
                api_core.exceptions.ServiceUnavailable,
                api_core.exceptions.Unknown,
                api_core.exceptions.Cancelled,
            ),
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        for n in range(1, 10):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            future = publisher.publish(topic=topic_path, data=data, retry=custom_retry)
            print(future.result())

        print(f"Published messages with retry settings to {topic_path}.")
        # [END pubsub_publisher_retry_settings]


    def publish_with_ordering_keys(project_id, topic_id):
        """Publishes messages with ordering keys."""
        # [START pubsub_publish_with_ordering_keys]
        from google.cloud import pubsub_v1

        # TODO(developer): Choose an existing topic.
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
        # Sending messages to the same region ensures they are received in order
        # even when multiple publishers are used.
        client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
        publisher = pubsub_v1.PublisherClient(
            publisher_options=publisher_options, client_options=client_options
        )
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)

        for message in [
            ("message1", "key1"),
            ("message2", "key2"),
            ("message3", "key1"),
            ("message4", "key2"),
        ]:
            # Data must be a bytestring
            data = message[0].encode("utf-8")
            ordering_key = message[1]
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data=data, ordering_key=ordering_key)
            print(future.result())

        print(f"Published messages with ordering keys to {topic_path}.")
        # [END pubsub_publish_with_ordering_keys]


    def resume_publish_with_ordering_keys(project_id, topic_id):
        """Resume publishing messages with ordering keys when unrecoverable errors occur."""
        # [START pubsub_resume_publish_with_ordering_keys]
        from google.cloud import pubsub_v1

        # TODO(developer): Choose an existing topic.
        # project_id = "your-project-id"
        # topic_id = "your-topic-id"

        publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
        # Sending messages to the same region ensures they are received in order
        # even when multiple publishers are used.
        client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
        publisher = pubsub_v1.PublisherClient(
            publisher_options=publisher_options, client_options=client_options
        )
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)

        for message in [
            ("message1", "key1"),
            ("message2", "key2"),
            ("message3", "key1"),
            ("message4", "key2"),
        ]:
            # Data must be a bytestring
            data = message[0].encode("utf-8")
            ordering_key = message[1]
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data=data, ordering_key=ordering_key)
            try:
                print(future.result())
            except RuntimeError:
                # Resume publish on an ordering key that has had unrecoverable errors.
                publisher.resume_publish(topic_path, ordering_key)

        print(f"Resumed publishing messages with ordering keys to {topic_path}.")
        # [END pubsub_resume_publish_with_ordering_keys]


    def detach_subscription(project_id, subscription_id):
        """Detaches a subscription from a topic and drops all messages retained in it."""
        # [START pubsub_detach_subscription]
        from google.api_core.exceptions import GoogleAPICallError, RetryError
        from google.cloud import pubsub_v1

        # TODO(developer): Choose an existing subscription.
        # project_id = "your-project-id"
        # subscription_id = "your-subscription-id"

        publisher_client = pubsub_v1.PublisherClient()
        subscriber_client = pubsub_v1.SubscriberClient()
        subscription_path = subscriber_client.subscription_path(project_id, subscription_id)

        try:
            publisher_client.detach_subscription(
                request={"subscription": subscription_path}
            )
        except (GoogleAPICallError, RetryError, ValueError, Exception) as err:
            print(err)

        subscription = subscriber_client.get_subscription(
            request={"subscription": subscription_path}
        )
        if subscription.detached:
            print(f"{subscription_path} is detached.")
        else:
            print(f"{subscription_path} is NOT detached.")
        # [END pubsub_detach_subscription]

class Channel(virtual.Channel):

    """PubSub Channel """
    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        topics = self.Functions.list_topics(prefix=self.queue_name_prefix)
        for queue in topics:
            self._queue_cache[queue.name] = queue
        self._fanout_topics = set()
    """    
    def basic_consume(self, queue, no_ack, *args, **kwargs):
        pass
    def basic_cancel(self, consumer_tag):
        # this can be done later
        pass
    def drain_events(self, timeout=None, callback=None, **kwargs):
        pass
    def entity_name(...):
        pass
    # Modules related to creating subscriptions
    """
    @property
    def conninfo(self):
        # return self.connection.client

    """
    @property
    def transport_options(self):
        # return self.connection.client.transport_options
    @cached_property
    def visibility_timeout(self):
        # return (self.transport_options.get('visibility_timeout') or
        #         self.default_visibility_timeout)
    @cached_property
    def predefined_queues(self):
        """Map of queue_name to predefined queue settings."""
        # return self.transport_options.get('predefined_queues', None)
    @cached_property
    def queue_name_prefix(self):
        # return self.transport_options.get('queue_name_prefix', '')
    @cached_property
    def supports_fanout(self):
        return False
    @cached_property
    def region(self):
        # return (self.transport_options.get('region') or
        #         boto3.Session().region_name or
        #         self.default_region)
    @cached_property
    def regioninfo(self):
        # return self.transport_options.get('regioninfo')
    @cached_property
    def is_secure(self):
        # return self.transport_options.get('is_secure')
    @cached_property
    def port(self):
        # return self.transport_options.get('port')
    @cached_property
    def endpoint_url(self):
        # if self.conninfo.hostname is not None:
        #     scheme = 'https' if self.is_secure else 'http'
        #     if self.conninfo.port is not None:
        #         port = f':{self.conninfo.port}'
        #     else:
        #         port = ''
        #     return '{}://{}{}'.format(
        #         scheme,
        #         self.conninfo.hostname,
        #         port
        #     )
    @cached_property
    def wait_time_seconds(self):
        # return self.transport_options.get('wait_time_seconds',
        #                                   self.default_wait_time_seconds)
"""
    def _get_regioninfo(self, regions):
        if self.region:
            for _r in regions:
                if _r.name == self.region:
                    return _r

    def _pubsub_connect_to(self, fun, regions):
        conninfo = self.conninfo
        region = self._get_regioninfo(regions)
        return fun(region=region,
                pubsub_access_key=conninfo.userid,
                port=conninfo.port)

class Transport(virtual.Transport):
    """PubSub Transport.
    .. code-block:: python
        from kombu.transport.pubsub import Transport
        // TODO: this needs to be refactored
        transport = Transport(
            ...,
            transport_options={
                'sqs-creation-attributes': {
                    'KmsMasterKeyId': 'alias/aws/sqs',
                },
            }
        )
    """  # noqa: E501
    # TODO: all below implementation needs to be checked and
    #   set according to google pub sub.
    # Channel = Channel
    # polling_interval = 1
    # wait_time_seconds = 0
    # default_port = None
    # connection_errors = (
    #     virtual.Transport.connection_errors +
    #     (exceptions.BotoCoreError, socket.error)
    # )
    # channel_errors = (
    #     virtual.Transport.channel_errors + (exceptions.BotoCoreError,)
    # )
    # driver_type = 'sqs'
    # driver_name = 'sqs'
    # implements = virtual.Transport.implements.extend(
    #     asynchronous=True,
    #     exchange_type=frozenset(['direct']),
    # )
    @property
    def default_connection_params(self):
        return {'port': self.default_port}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Your Google Cloud project ID")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list", help=list_topics.__doc__)

    create_parser = subparsers.add_parser("create", help=create_topic.__doc__)
    create_parser.add_argument("topic_id")

    delete_parser = subparsers.add_parser("delete", help=delete_topic.__doc__)
    delete_parser.add_argument("topic_id")

    publish_parser = subparsers.add_parser("publish", help=publish_messages.__doc__)
    publish_parser.add_argument("topic_id")

    publish_with_custom_attributes_parser = subparsers.add_parser(
        "publish-with-custom-attributes",
        help=publish_messages_with_custom_attributes.__doc__,
    )
    publish_with_custom_attributes_parser.add_argument("topic_id")

    publish_with_error_handler_parser = subparsers.add_parser(
        "publish-with-error-handler", help=publish_messages_with_error_handler.__doc__,
    )
    publish_with_error_handler_parser.add_argument("topic_id")

    publish_with_batch_settings_parser = subparsers.add_parser(
        "publish-with-batch-settings",
        help=publish_messages_with_batch_settings.__doc__,
    )
    publish_with_batch_settings_parser.add_argument("topic_id")

    publish_with_retry_settings_parser = subparsers.add_parser(
        "publish-with-retry-settings",
        help=publish_messages_with_retry_settings.__doc__,
    )
    publish_with_retry_settings_parser.add_argument("topic_id")

    publish_with_ordering_keys_parser = subparsers.add_parser(
        "publish-with-ordering-keys", help=publish_with_ordering_keys.__doc__,
    )
    publish_with_ordering_keys_parser.add_argument("topic_id")

    resume_publish_with_ordering_keys_parser = subparsers.add_parser(
        "resume-publish-with-ordering-keys",
        help=resume_publish_with_ordering_keys.__doc__,
    )
    resume_publish_with_ordering_keys_parser.add_argument("topic_id")

    detach_subscription_parser = subparsers.add_parser(
        "detach-subscription", help=detach_subscription.__doc__,
    )
    detach_subscription_parser.add_argument("subscription_id")

    args = parser.parse_args()

    if args.command == "list":
        list_topics(args.project_id)
    elif args.command == "create":
        create_topic(args.project_id, args.topic_id)
    elif args.command == "delete":
        delete_topic(args.project_id, args.topic_id)
    elif args.command == "publish":
        publish_messages(args.project_id, args.topic_id)
    elif args.command == "publish-with-custom-attributes":
        publish_messages_with_custom_attributes(args.project_id, args.topic_id)
    elif args.command == "publish-with-error-handler":
        publish_messages_with_error_handler(args.project_id, args.topic_id)
    elif args.command == "publish-with-batch-settings":
        publish_messages_with_batch_settings(args.project_id, args.topic_id)
    elif args.command == "publish-with-retry-settings":
        publish_messages_with_retry_settings(args.project_id, args.topic_id)
    elif args.command == "publish-with-ordering-keys":
        publish_with_ordering_keys(args.project_id, args.topic_id)
    elif args.command == "resume-publish-with-ordering-keys":
        resume_publish_with_ordering_keys(args.project_id, args.topic_id)
    elif args.command == "detach-subscription":
        detach_subscription(args.project_id, args.subscription_id)