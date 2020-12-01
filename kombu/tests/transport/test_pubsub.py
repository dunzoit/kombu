import unittest
from mock import patch, call, Mock, MagicMock, PropertyMock

from kombu.transport.pubsub import Channel
from kombu.exceptions import ChannelError
from google.api_core.exceptions import AlreadyExists


class InnerMsg(object):
    def __init__(self, id, data=None):
        self.data = data
        self.message_id = id


class OuterMsg(object):
    def __init__(self, id):
        self.message = InnerMsg(id)


class TestChannel(unittest.TestCase):
    ''' TestChannel '''

    def setUp(self):
        ''' setUp '''
        mkConn = MagicMock()
        mkid = mkConn.return_value._avail_channel_ids = MagicMock()
        mkid.return_value.pop = MagicMock(return_value="foo")
        self.mocktrans = mkConn.client.\
            transport_options = MagicMock(return_value={})
        mkConn.return_value.QoS = MagicMock()
        self.channel = Channel(mkConn)

    @patch('kombu.transport.pubsub.Channel.project_id', new_callable=PropertyMock)
    @patch('kombu.transport.pubsub.Channel.subscriber', new_callable=PropertyMock)
    def test__new_queue_from_client(self, mkSub, mkID):
        ''' test__new_queue_from_client '''
        mkID.return_value = "fizzbuzz"
        path = mkSub.return_value.subscription_path = MagicMock(
            return_value="/foo/bar"
        )
        subscription_path = self.channel._new_queue("foo")
        self.assertEquals(subscription_path, "/foo/bar")
        path.assert_called_with("fizzbuzz", "foo")

    def test__new_queue_from_cache(self):
        ''' test__new_queue_from_cache '''
        self.channel._queue_cache = {"foo": "QueueFoo"}
        subscription_path = self.channel._new_queue("foo")
        self.assertEquals(subscription_path, "QueueFoo")

    def test__get_from_subscription_pull(self):
        ''' test__get_from_subscription_pull '''
        msg1, msg2 = OuterMsg(1), OuterMsg(2)
        with patch('kombu.transport.pubsub.Channel.subscriber',
                   new_callable=PropertyMock) as mkSub:
            with patch('kombu.transport.pubsub.Channel.qos',
                       new_callable=PropertyMock) as mkQoS:
                append = mkQoS.return_value.append = MagicMock()
                newQ = self.channel._new_queue = MagicMock(
                    return_value="foo/bar")
                resp = MagicMock()
                resp.received_messages = [msg1, msg2]
                mkSub.return_value.pull = MagicMock(return_value=resp)
                qosCalls = [
                    call(1, (msg1, "foo/bar")),
                    call(2, (msg2, "foo/bar"))
                ]
                msg = self.channel._get("foo")
                newQ.assert_called_with("foo")
                append.assert_has_calls(qosCalls)
                self.assertIsInstance(msg, OuterMsg)
                self.assertEqual(msg.message.message_id, 1)

    def test__get_from_temp_cache(self):
        ''' test__get_from_temp_cache '''
        self.channel.temp_cache.put(OuterMsg(1))
        msg = self.channel._get("foo")
        self.assertIsInstance(msg, OuterMsg)
        self.assertEqual(msg.message.message_id, 1)

    def test_queue_declare_successful(self):
        ''' test_queue_declare_successful '''
        newQ = self.channel._new_queue = MagicMock()
        with patch('kombu.transport.pubsub.uuid') as mkID:
            with patch('kombu.transport.pubsub.queue_declare_ok_t') as mkQok:
                mkQok.return_value = "ok"
                mkID.return_value = "foo"
                rVal = self.channel.queue_declare(queue="test")
                self.assertEqual(rVal, "ok")
                newQ.assert_called_with("test")
                mkQok.assert_called_with("test", 0, 0)

    def test_queue_declare_raises_exception(self):
        ''' test_queue_declare_raises_exception '''
        with patch('kombu.transport.pubsub.uuid') as mkID:
            mkID.return_value = "foo"
            with self.assertRaises(ChannelError):
                self.channel.queue_declare(
                    queue="test", passive=True)

    def test_queue_bind_creates_subscription(self):
        ''' test_queue_bind_creates_subscription '''
        with patch('kombu.transport.pubsub.Channel.subscriber',
                   new_callable=PropertyMock) as mkSub:
            with patch('kombu.transport.pubsub.Channel.ack_deadline_seconds',
                       new_callable=PropertyMock) as mkAck:
                with patch('kombu.transport.pubsub.Channel.state') as mkState:
                    mkState.exchanges = {"test_ex": "TEST_EX"}
                    mkAck.return_value = 60
                    self.channel._new_queue = MagicMock(return_value="foo")
                    subcription = mkSub.return_value.create_subscription =\
                        MagicMock(return_value="/foo/bar")
                    self.channel.\
                        queue_bind(queue="test_q", exchange="test_ex")
                    subcription.assert_called_with(
                        "foo", "TEST_EX", ack_deadline_seconds=60)

    def test_queue_bind_already_exists(self):
        ''' test_queue_bind_already_exists '''
        with patch('kombu.transport.pubsub.Channel.subscriber',
                   new_callable=PropertyMock) as mkSub:
            with patch('kombu.transport.pubsub.Channel.ack_deadline_seconds',
                       new_callable=PropertyMock) as mkAck:
                with patch('kombu.transport.pubsub.Channel.state') as mkState:
                    mkState.exchanges = {"test_ex": "TEST_EX"}
                    mkAck.return_value = 60
                    self.channel._new_queue = MagicMock(return_value="foo")
                    mkCreate = mkSub.return_value.create_subscription =\
                        Mock(side_effect=AlreadyExists(1))
                    rVal = self.channel.\
                        queue_bind(queue="test_q", exchange="test_ex")
                    mkCreate.assert_called_with(
                        "foo", "TEST_EX", ack_deadline_seconds=60)
                    self.assertIsNone(rVal)

    def test_queue_bind_raises_exception(self):
        ''' test_queue_bind_raises_exception '''
        with patch('kombu.transport.pubsub.Channel.subscriber',
                   new_callable=PropertyMock) as mkSub:
            with patch('kombu.transport.pubsub.Channel.ack_deadline_seconds',
                       new_callable=PropertyMock) as mkAck:
                with patch('kombu.transport.pubsub.Channel.state',
                           new_callable=PropertyMock) as mkState:
                    mkState.exchanges = {"test_ex": "TEST_EX"}
                    mkAck.return_value = 60
                    self.channel._new_queue = MagicMock(return_value="foo")
                    mkCreate = mkSub.return_value.create_subscription =\
                        Mock(side_effect=Exception)
                    with self.assertRaises(Exception):
                        self.channel.\
                            queue_bind(queue="test_q", exchange="test_ex")
                        mkCreate.assert_called_with(
                            "foo", "TEST_EX", ack_deadline_seconds=60)

    @patch('kombu.transport.pubsub.Channel.state', new_callable=PropertyMock)
    def test_exchange_declare_create_topic(self, mkState):
        ''' test_exchange_declare_create_topic '''
        with patch('kombu.transport.pubsub.Channel.publisher',
                   new_callable=PropertyMock) as mkPub:
            with patch('kombu.transport.pubsub.Channel.project_id',
                       new_callable=PropertyMock) as mkID:
                mkID.return_value = "test_project_id"
                mkState.return_value.exchanges = {}
                path = mkPub.return_value.topic_path =\
                    MagicMock(return_value="topic/foo")
                topic = mkPub.return_value.create_topic = MagicMock()
                self.channel.exchange_declare(exchange="test_ex")
                path.assert_called_with("test_project_id", "test_ex")
                topic.assert_called_with("topic/foo")
                self.assertEqual(
                    mkState.return_value.exchanges["test_ex"], "topic/foo")

    @patch('kombu.transport.pubsub.Channel.state', new_callable=PropertyMock)
    def test_exchange_declare_already_exists(self, mkState):
        ''' test_exchange_declare_already_exists '''
        with patch('kombu.transport.pubsub.Channel.publisher',
                   new_callable=PropertyMock) as mkPub:
            with patch('kombu.transport.pubsub.Channel.project_id',
                       new_callable=PropertyMock) as mkID:
                mkID.return_value = "test_project_id"
                mkState.return_value.exchanges = {}
                path = mkPub.return_value.topic_path =\
                    MagicMock(return_value="topic/foo")
                topic = mkPub.return_value.create_topic = MagicMock(
                    side_effect=AlreadyExists(1))
                self.channel.exchange_declare(exchange="test_ex")
                path.assert_called_with("test_project_id", "test_ex")
                topic.assert_called_with("topic/foo")
                self.assertEqual(
                    mkState.return_value.exchanges["test_ex"], "topic/foo")

    @patch('kombu.transport.pubsub.Channel.state', new_callable=PropertyMock)
    def test_exchange_declare_raises_expection(self, mkState):
        ''' test_exchange_declare_raises_expection '''
        with patch('kombu.transport.pubsub.Channel.publisher',
                   new_callable=PropertyMock) as mkPub:
            with patch('kombu.transport.pubsub.Channel.project_id',
                       new_callable=PropertyMock) as mkID:
                mkID.return_value = "test_project_id"
                mkState.return_value.exchanges = {}
                path = mkPub.return_value.topic_path =\
                    MagicMock(return_value="topic/foo")
                topic = mkPub.return_value.create_topic = MagicMock(
                    side_effect=Exception)
                with self.assertRaises(Exception):
                    self.channel.exchange_declare(exchange="test_ex")
                    path.assert_called_with("test_project_id", "test_ex")
                    topic.assert_called_with("topic/foo")

    @patch('kombu.transport.pubsub.dumps')
    def test_basic_publish(self, mkDumps):
        ''' test_basic_publish '''
        mkDumps.return_value = '{"foo": "bar"}'
        with patch('kombu.transport.pubsub.Channel.publisher',
                   new_callable=PropertyMock) as mkPub:
            with patch('kombu.transport.pubsub.Channel.project_id',
                       new_callable=PropertyMock) as mkID:
                mkID.return_value = "test_project_id"
                path = mkPub.return_value.topic_path = MagicMock(
                    return_value="topic/foo")
                future = MagicMock()
                future.result = MagicMock(return_value="foo")
                publish = mkPub.return_value.publish = MagicMock(
                    return_value=future)
                rVal = self.channel.basic_publish(
                    {"foo": "bar"}, exchange="test_ex")
                path.assert_called_with("test_project_id", "test_ex")
                mkDumps.assert_called_with({"foo": "bar"})
                publish.assert_called_with("topic/foo", '{"foo": "bar"}')
                self.assertEqual(rVal, "foo")

    @patch('google.cloud.pubsub_v1.PublisherClient')
    def test_publisher(self, mockPub):
        ''' test_publisher '''
        self.channel.publisher
        mockPub.assert_called()

    @patch('google.cloud.pubsub_v1.SubscriberClient')
    def test_subscriber(self, mockSub):
        ''' test_publisher '''
        self.channel.subscriber
        mockSub.assert_called()

    def test_transport_options(self):
        ''' test_transport_options '''
        out = self.channel.transport_options
        self.assertEquals(out, self.mocktrans)

    def test_project_id(self):
        ''' test_project_id '''
        mock_out = self.mocktrans.get.return_value = {
            'PROJECT_ID': 'mock_project_id'}
        out = self.channel.project_id
        self.assertEquals(out, mock_out)

    def test_max_messages(self):
        ''' test_max_messages '''
        mock_out = self.mocktrans.get.return_value = {
            'PROJECT_ID': 'mock_project_id'}
        out = self.channel.max_messages
        self.assertEquals(out, mock_out)

    def test_ack_deadline_seconds(self):
        ''' test_ack_deadline_seconds '''
        mock_out = self.mocktrans.get.return_value = {
            'PROJECT_ID': 'mock_project_id'}
        out = self.channel.ack_deadline_seconds
        self.assertEquals(out, mock_out)
