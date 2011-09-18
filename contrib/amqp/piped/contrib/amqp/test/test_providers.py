# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

import mock
from mock import patch
from pika import frame, exceptions as pika_exceptions
from twisted.application import service
from twisted.python import failure
from twisted.trial import unittest
from twisted.internet import defer, error

from piped import util, processing, dependencies, exceptions, event
from piped.contrib.amqp import providers


class TestConnectionProvider(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.configure()

    def tearDown(self):
        self.runtime_environment.application.stopService()

    def test_provided_connections(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('amqp.connections.test_connection', dict(
            servers = ['tcp:host=localhost:port=5672'],
            max_idle_time = 10,
            reconnect_interval = 5,
            parameters = dict(
                heartbeat = 3
            )
        ))

        provider = providers.AMQPConnectionProvider()
        provider.configure(self.runtime_environment)

        connection_dependency = dependencies.ResourceDependency(provider='amqp.connection.test_connection')
        self.runtime_environment.resource_manager.resolve(connection_dependency)

        with patch.object(providers.endpoints, 'clientFromString') as mocked_client_from_string:
            def connect(factory):
                protocol = factory.buildProtocol(None)
                protocol.transport = mock.Mock()
                # mark the protocol as ready
                protocol.connectionReady(protocol)
                return protocol

            mocked_client_from_string.return_value.connect.side_effect = connect
            provider.startService()

            connection = connection_dependency.get_resource()
            self.assertIsInstance(connection, providers.AMQProtocol)

            self.assertEquals(connection.factory.servers, cm.get('amqp.connections.test_connection.servers'))
            self.assertEquals(connection.factory.reconnect_interval, cm.get('amqp.connections.test_connection.reconnect_interval'))
            self.assertEquals(connection.idle_checker.interval, cm.get('amqp.connections.test_connection.max_idle_time'))
            self.assertEquals(connection.parameters.heartbeat, cm.get('amqp.connections.test_connection.parameters.heartbeat'))

        # if another dependency asks for the same connection, it should receive the same instance
        another_connection_dependency = dependencies.ResourceDependency(provider='amqp.connection.test_connection')
        self.runtime_environment.resource_manager.resolve(another_connection_dependency)

        self.assertIdentical(another_connection_dependency.get_resource(), connection_dependency.get_resource())


class TestMockConnection(unittest.TestCase):

    def setUp(self):
        self.service = service.MultiService()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.service.stopService()

    @defer.inlineCallbacks
    def test_get_named_channels(self):
        connection = providers.AMQPConnection('test_name', servers=['tcp:host=server_1:port=5672'])
        connection.setServiceParent(self.service)

        with patch.object(providers.endpoints, 'clientFromString') as mocked_client_from_string:
            def connect(factory):
                protocol = factory.buildProtocol(None)
                protocol.transport = mock.Mock()
                # mark the protocol as ready
                protocol.connectionReady(protocol)
                return protocol

            mocked_client_from_string.return_value.connect.side_effect = connect
            connection.startService()

            # the connection should now be finished connecting:
            self.assertTrue(connection.ready)

            protocol = connection.protocol

            channel_requests = list()
            with patch.object(protocol, 'channel') as mocked_channel:
                def channel():
                    d = defer.Deferred()
                    channel_requests.append(d)
                    return d

                mocked_channel.side_effect = channel

                foo_1_d = protocol.get_named_channel('foo')
                foo_2_d = protocol.get_named_channel('foo')
                foo_3_d = protocol.get_named_channel('foo')

                bar_d = protocol.get_named_channel('bar')

                self.assertEquals(len(channel_requests), 2)

                mocked_foo = mock.Mock()
                mocked_foo_close = event.Event()
                mocked_foo.add_on_close_callback.side_effect = mocked_foo_close.handle
                mocked_bar = mock.Mock()

                channel_requests.pop(0).callback(mocked_foo)
                channel_requests.pop(0).callback(mocked_bar)

                # all the pending requests for the channel should receive the same instance
                foo = yield defer.DeferredList([foo_1_d, foo_2_d, foo_3_d])
                self.assertEquals(set(foo), set([(True, mocked_foo)]))

                bar = yield bar_d
                self.assertEquals(bar, mocked_bar)

                self.assertEquals(len(channel_requests), 0)
                another_foo = yield protocol.get_named_channel('foo')
                self.assertEquals(len(channel_requests), 0)

                # if the channel is closed, the next request to get the channel will recreate it
                mocked_foo_close()
                self.assertEquals(len(channel_requests), 0)
                new_foo = protocol.get_named_channel('foo')
                self.assertEquals(len(channel_requests), 1)

                # if the channel request errbacks, all pending requests for the named channel should errback
                new_foo_2 = protocol.get_named_channel('foo')
                test_exception = Exception()
                channel_requests.pop(0).callback(failure.Failure(test_exception))

                for pending_foo in (new_foo, new_foo_2):
                    try:
                        yield pending_foo
                        self.fail('Expected {0!r} to be raised.'.format(new_foo))
                    except Exception as e:
                        self.assertEquals(e, test_exception)


    def test_connect_disconnect_when_service_starts_stops(self):
        connection = providers.AMQPConnection('test_name', servers=['tcp:host=localhost:port=5672'])

        with patch.object(connection, '_connect') as mocked_connect:
            def verify(endpoint):
                self.assertEquals(endpoint._host, 'localhost')
                self.assertEquals(endpoint._port, 5672)

            mocked_connect.side_effect = verify
            connection.startService()

            mocked_connect.assert_called_once()

        with patch.object(connection, '_disconnect') as mocked_disconnect:
            connection.stopService()

            mocked_disconnect.assert_called_once_with('stopping service')

    @defer.inlineCallbacks
    def test_reconnect_if_connect_fails(self):
        connection = providers.AMQPConnection('test_name', servers=['tcp:host=server_1:port=5672', 'tcp:host=server_2:port=5673'], reconnect_interval=0)
        connection.setServiceParent(self.service)

        endpoints = list()
        connect_return_values = [error.ConnectError('test_error'), error.DNSLookupError('test_error'), error.ConnectionDone('test_error')]
        expected_errors = list(connect_return_values)

        disconnects = list()
        connection.on_disconnected += disconnects.append

        with patch.object(connection, '_connect') as mocked_connect:
            def verify(endpoint):
                endpoints.append(dict(host=endpoint._host, port=endpoint._port))
                raise connect_return_values.pop(0)

            mocked_connect.side_effect = verify

            with patch.object(providers.log, 'warn') as mocked_warn:
                connection.startService()
                while connect_return_values:
                    yield util.wait(0)

                self.assertEquals(mocked_warn.call_count, len(expected_errors))

            # the servers should be attempted in a round-robin fashion:
            server_1 = dict(host='server_1', port=5672)
            server_2 = dict(host='server_2', port=5673)
            self.assertEquals(endpoints, [server_1, server_2, server_1])

            # all our expected errors should be seen as reasons for disconnects:
            errors = [disconnect.value for disconnect in disconnects]
            self.assertEquals(errors, expected_errors)

    def test_reconnect_on_connection_lost(self):
        connection = providers.AMQPConnection('test_name', servers=['tcp:host=server_1:port=5672'])
        connection.setServiceParent(self.service)

        # store the connected/disconnected events
        events = list()
        connection.on_connected += events.append
        connection.on_disconnected += events.append

        with patch.object(providers.endpoints, 'clientFromString') as mocked_client_from_string:
            def connect(factory):
                protocol = factory.buildProtocol(None)
                protocol.transport = mock.Mock()
                # mark the protocol as ready
                protocol.connectionReady(protocol)
                return protocol

            mocked_client_from_string.return_value.connect.side_effect = connect
            connection.startService()

            # the connection should now be finished connecting:
            self.assertTrue(connection.ready)
            self.assertEquals(connection._connecting, None)
            self.assertEquals(connection._reconnecting, None)

            # tell the protocol the connection has been lost
            previous_protocol = connection.protocol
            previous_protocol.connectionLost('testing lost')

            # the connection will be retried immediately:
            self.assertTrue(connection.ready)
            self.assertEquals(connection._connecting, None)
            self.assertEquals(connection._reconnecting, None)

            # the connection should now be using a new protocol
            self.assertNotEquals(previous_protocol, connection.protocol)

            self.assertEquals(events, [previous_protocol, 'testing lost', connection.protocol])

    @defer.inlineCallbacks
    def test_idle_connections_are_disconnected(self):
        # create a connection with max_idle_time set to 0, which means that data must be sent or received every reactor iteration.
        connection = providers.AMQPConnection('test_name', servers=['tcp:host=server_1:port=5672'], max_idle_time=0)
        connection.setServiceParent(self.service)

        # store the connected/disconnected events
        events = list()
        connection.on_connected += events.append
        connection.on_disconnected += events.append

        with patch.object(providers.endpoints, 'clientFromString') as mocked_client_from_string:
            created_protocols = defer.DeferredQueue()
            def connect(factory):
                protocol = factory.buildProtocol(None)
                protocol.transport = mock.Mock()
                protocol.transport.loseConnection.side_effect = lambda: protocol.connectionLost('test lost connection')

                created_protocols.put(protocol)

                protocol.connectionReady('test ready')
                return protocol

            mocked_client_from_string.return_value.connect.side_effect = connect
            connection.startService()

            # protocols 1 and 2 gets disconnected by the idle checker, and we will examine protocol 3 closer:
            protocol_1 = yield created_protocols.get()
            protocol_2 = yield created_protocols.get()
            protocol_3 = yield created_protocols.get()

            # wait one reactor iteration for the connection to start using the third protocol
            yield util.wait(0)

            # the third attempt has not resulted in an on_connection() event yet
            self.assertEquals(events, [protocol_1, 'test lost connection', protocol_2, 'test lost connection', protocol_3])
            events[:] = list()

            # send a heartbeat frame every reactor iteration in order to pretend that data is being transferred
            for i in range(10):
                protocol_3._send_frame(frame.Heartbeat())
                yield util.wait(0)

            # since the protocol haven't been idle, it should still be used by the connection
            self.assertEquals(events, list())
            self.assertEquals(created_protocols.pending, list())

            # .. but if we stop manually bumping the received bytes count, it should die:
            yield util.wait(0)

            # .. which causes a new protocol to be created immediately
            self.assertEquals(len(created_protocols.pending), 1)
            self.assertEquals(events, ['test lost connection', created_protocols.pending[0]])


class TestConsumerProvider(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.configure()

    def tearDown(self):
        self.runtime_environment.application.stopService()

    def test_consumer_created(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('amqp.connections.bar.basic_consumers.test_consumer', dict(
            pipeline = 'foo'
        ))
        provider = providers.AMQPConsumerProvider()

        with patch.object(providers, 'AMQPConsumer') as mock_consumer_class:
            provider.configure(self.runtime_environment)
            mock_consumer_class.assert_called_once_with(name='test_consumer', pipeline='foo', connection='bar')


class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.configure()

    def tearDown(self):
        self.runtime_environment.application.stopService()

    def _create_consumer(self, **consumer_config):
        dm = self.runtime_environment.dependency_manager
        consumer = providers.AMQPConsumer('test consumer', **consumer_config)
        consumer.configure(self.runtime_environment)
        # set the dependencies as resolved, so their events (ready/etc) are propagated
        dm._dependency_graph.node[consumer.dependency]['resolved'] = True
        dm._dependency_graph.node[consumer.pipeline_dependency]['resolved'] = True
        dm._dependency_graph.node[consumer.connection_dependency]['resolved'] = True
        return consumer

    def test_queue_declaration(self):
        # Anonymous queues are exclusive by default
        consumer = self._create_consumer(
            pipeline='p', connection='c'
        )
        self.assertEquals(consumer.queue_declare['queue'], '')
        self.assertEquals(consumer.queue_declare['exclusive'], True)

        # queues can be a simple string-named queue, which means it
        # should be passive
        consumer = self._create_consumer(
            pipeline='p', connection='c',
            queue = 'foo'
        )
        self.assertEquals(consumer.queue_declare['queue'], 'foo')
        self.assertEquals(consumer.queue_declare['exclusive'], False)

        # we can use a dict to be more explicit about the queue configuration.
        consumer = self._create_consumer(
            pipeline='p', connection='c',
            queue = dict(
                queue = 'bar',
                exclusive = True
            )
        )
        self.assertEquals(consumer.queue_declare['queue'], 'bar')
        self.assertEquals(consumer.queue_declare['exclusive'], True)

    def test_invalid_configurations(self):
        self.assertRaises(exceptions.ConfigurationError, self._create_consumer,
            pipeline='p', connection='c',
            ack_before_processing = True,
            ack_after_successful_processing = True,
            nack_after_failed_processing = True
        )

        self.assertRaises(exceptions.ConfigurationError, self._create_consumer,
            pipeline='p', connection='c',
            ack_before_processing = True,
            ack_after_successful_processing = False,
            nack_after_failed_processing = True
        )

        self.assertRaises(exceptions.ConfigurationError, self._create_consumer,
            pipeline='p', connection='c',
            ack_before_processing = True,
            ack_after_successful_processing = True,
            nack_after_failed_processing = False
        )

    def test_messages_are_delivered_to_pipelines(self):
        consumer = self._create_consumer(pipeline='test_pipeline', connection='test_connection', qos=dict(prefetch_count=4))
        consumer.startService()

        mocked_pipeline = mock.Mock(name='pipeline')
        consumer.pipeline_dependency.on_resource_ready(mocked_pipeline)

        message_queue = defer.DeferredQueue()
        mocked_connection = mock.Mock(name='connection')
        mocked_channel = mocked_connection.channel.return_value = mock.Mock(name='channel')
        mocked_consumer_tag = mock.Mock(name='consumer_tag')
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        consumer.connection_dependency.on_resource_ready(mocked_connection)

        mocked_channel.basic_qos.assert_called_once_with(prefetch_count=4)

        # no messages have entered the queue yet:
        self.assertEquals(mocked_pipeline.process.call_count, 0)

        # but putting message into the queue should result in the pipeline being invoked
        message_queue.put((mocked_channel, mock.Mock(name='method'), mock.Mock(name='properties'), 'test message body'))
        self.assertEquals(mocked_pipeline.process.call_count, 1)

        # call_args_list is a list of (args, kwargs) tuples, and the baton is the first element in the argument
        baton = mocked_pipeline.process.call_args_list[0][0][0]

        self.assertEquals(baton['channel'], mocked_channel)
        self.assertEquals(baton['method']._name, 'method')
        self.assertEquals(baton['properties']._name, 'properties')
        self.assertEquals(baton['body'], 'test message body')

        # since the processing worked, the message should have been acked
        mocked_channel.basic_ack.assert_called_once_with(delivery_tag=baton['method'].delivery_tag)

    def test_messages_are_delivered_to_pipelines_after_restarting(self):
        consumer = self._create_consumer(pipeline='test_pipeline', connection='test_connection')
        consumer.startService()

        mocked_pipeline = mock.Mock(name='pipeline')
        consumer.pipeline_dependency.on_resource_ready(mocked_pipeline)

        message_queue = defer.DeferredQueue()
        mocked_connection = mock.Mock(name='connection')
        mocked_channel = mocked_connection.channel.return_value = mock.Mock(name='channel')
        mocked_consumer_tag = mock.Mock(name='consumer_tag')
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        consumer.connection_dependency.on_resource_ready(mocked_connection)

        self.assertEquals(mocked_pipeline.process.call_count, 0)
        message_queue.put((mocked_channel, mock.Mock(name='method'), mock.Mock(name='properties'), 'test message body'))
        self.assertEquals(mocked_pipeline.process.call_count, 1)

        with patch.object(providers.log, 'warn') as mocked_warn:
            consumer.stopService()
            consumer.startService()

        # but putting message into the queue should result in the pipeline being invoked
        self.assertEquals(mocked_pipeline.process.call_count, 1)
        message_queue.put((mocked_channel, mock.Mock(name='method'), mock.Mock(name='properties'), 'test message body'))
        self.assertEquals(mocked_pipeline.process.call_count, 2)

    @defer.inlineCallbacks
    def test_channels_are_reopened(self):
        # reopen channels after one reactor iteration
        consumer = self._create_consumer(pipeline='test_pipeline', connection='test_connection', channel_reopen_interval=0)
        consumer.startService()

        mocked_pipeline = mock.Mock(name='pipeline')
        consumer.pipeline_dependency.on_resource_ready(mocked_pipeline)

        # mock the queue so we can fake a closed channel
        message_queue = defer.DeferredQueue()
        mocked_connection = mock.Mock(name='connection')
        mocked_channel = mocked_connection.channel.return_value = mock.Mock(name='channel')
        mocked_consumer_tag = mock.Mock(name='consumer_tag')
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        consumer.connection_dependency.on_resource_ready(mocked_connection)

        self.assertEquals(mocked_pipeline.process.call_count, 0)
        message_queue.put((mocked_channel, mock.Mock(name='method'), mock.Mock(name='properties'), 'test message body'))
        self.assertEquals(mocked_pipeline.process.call_count, 1)

        # only one channel should have been created at the moment
        self.assertEquals(mocked_connection.channel.call_count, 1)

        with patch.object(providers.log, 'warn') as mocked_warn:
            message_queue.put(failure.Failure(pika_exceptions.ChannelClosed()))

        # wait for the reopen interval to pass:
        yield util.wait(0)
        # now, a second channel should have been made
        self.assertEquals(mocked_connection.channel.call_count, 2)

        self.assertEquals(mocked_pipeline.process.call_count, 1)
        message_queue.put((mocked_channel, mock.Mock(name='method'), mock.Mock(name='properties'), 'test message body'))
        self.assertEquals(mocked_pipeline.process.call_count, 2)

    def test_message_nacking_on_failed_processing(self):
        consumer = self._create_consumer(pipeline='test_pipeline', connection='test_connection')
        consumer.startService()

        mocked_pipeline = mock.Mock(name='pipeline')
        consumer.pipeline_dependency.on_resource_ready(mocked_pipeline)

        message_queue = defer.DeferredQueue()
        mocked_connection = mock.Mock(name='connection')
        mocked_channel = mocked_connection.channel.return_value = mock.Mock(name='channel')
        mocked_consumer_tag = mock.Mock(name='consumer_tag')
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        consumer.connection_dependency.on_resource_ready(mocked_connection)

        # no messages have entered the queue yet:
        self.assertEquals(mocked_pipeline.process.call_count, 0)

        def raiser(baton):
            raise Exception('test exception')

        mocked_pipeline.process.side_effect = raiser

        # but putting message into the queue should result in the pipeline being invoked
        with patch.object(providers.log, 'warn') as mocked_warn:
            mocked_method = mock.Mock(name='method')
            message_queue.put((mocked_channel, mocked_method, mock.Mock(name='properties'), 'test message body'))
        
        self.assertEquals(mocked_pipeline.process.call_count, 1)
        
        # since the processing raised an exception, the message should have been rejected
        mocked_channel.basic_reject.assert_called_once_with(delivery_tag=mocked_method.delivery_tag)

    def test_message_acking_before_processing(self):
        consumer = self._create_consumer(pipeline='test_pipeline', connection='test_connection',
            ack_before_processing=True, ack_after_successful_processing=False,
            nack_after_failed_processing=False)
        consumer.startService()

        mocked_pipeline = mock.Mock(name='pipeline')
        consumer.pipeline_dependency.on_resource_ready(mocked_pipeline)

        message_queue = defer.DeferredQueue()
        mocked_connection = mock.Mock(name='connection')
        mocked_channel = mocked_connection.channel.return_value = mock.Mock(name='channel')
        mocked_consumer_tag = mock.Mock(name='consumer_tag')
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        consumer.connection_dependency.on_resource_ready(mocked_connection)

        # return a deferred that never fires
        mocked_pipeline.process.return_value = defer.Deferred()

        with patch.object(providers.log, 'warn') as mocked_warn:
            mocked_method = mock.Mock(name='method')
            message_queue.put((mocked_channel, mocked_method, mock.Mock(name='properties'), 'test message body'))

        self.assertEquals(mocked_pipeline.process.call_count, 1)

        # the message should be acked even if we're not done processing it
        mocked_channel.basic_ack.assert_called_once_with(delivery_tag=mocked_method.delivery_tag)