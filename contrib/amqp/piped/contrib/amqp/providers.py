# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import threading
import time
import math

import pika
from pika import frame, connection, exceptions as pika_exceptions, channel
from pika.adapters import twisted_connection
from twisted.application import service
from twisted.internet import reactor, defer, task, protocol, endpoints, error
from zope import interface
from twisted.python import reflect

from piped import exceptions, log, resource, dependencies, util, event


class AMQPConnectionProvider(object, service.MultiService):
    """ Provides AMQP connections. """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        self._connection_by_name = dict()
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        
        connections = runtime_environment.get_configuration_value('amqp.connections', dict())

        for connection_name, connection_config in connections.items():
            connection = AMQPConnection(connection_name, **connection_config)
            connection.setServiceParent(self)
            self._connection_by_name[connection_name] = connection

            logical_name = 'amqp.connection.{0}'.format(connection_name)

            runtime_environment.resource_manager.register(logical_name, provider=self)

    def add_consumer(self, resource_dependency):
        name = resource_dependency.provider.rsplit('.', 1)[-1]
        connection = self._connection_by_name[name]

        connection.on_connected += resource_dependency.on_resource_ready
        connection.on_disconnected += resource_dependency.on_resource_lost

        if connection.ready:
            resource_dependency.on_resource_ready(connection)


class AMQProtocol(twisted_connection.TwistedProtocolConnection):

    def __init__(self, parameters):
        super(AMQProtocol, self).__init__(parameters)

        # TODO: Remove this shim which makes heartbeating work similar
        #   how it is supposed to work with the twisted_connection.TwistedConnection
        if not self.ioloop:
            self.ioloop = twisted_connection.IOLoopReactorAdapter(self, reactor)
            self.ioloop.started = True
            self.ioloop.start = lambda: None
            self.ioloop.stop = lambda: None

        self.on_lost = event.Event()

    def _adapter_disconnect(self):
        self.transport.loseConnection()
        self._check_state_on_disconnect()

    def connectionLost(self, reason):
        self.on_lost(reason)
        return super(AMQProtocol, self).connectionLost(reason)


class AMQPConnection(object, service.MultiService):
    ready = False
    protocol = None
    _connecting = None

    def __init__(self, name, servers, max_idle_time=0, parameters=None):
        service.MultiService.__init__(self)
        
        self.name = name
        self.servers = servers
        self._server_index = -1
        
        parameters = parameters or dict()
        self.parameters = pika.ConnectionParameters(**parameters)

        self.on_connected = event.Event()
        self.on_disconnected = event.Event()

        self.on_connected += lambda _: setattr(self, 'ready', True)
        self.on_disconnected += lambda _: setattr(self, 'ready', False)
        self.on_disconnected += lambda _: setattr(self, 'protocol', None)

        self.max_idle_time = max_idle_time
        self._idle_state = (None, None)
        self.idle_checker = task.LoopingCall(self._check_idle)
        
        self.on_connected += lambda _: setattr(self, '_idle_state', (None, None))
        self.on_connected += lambda _: self.idle_checker.start(self.max_idle_time) if not self.idle_checker.running else None

        self.on_disconnected += lambda _: self.idle_checker.stop() if self.idle_checker.running else None
        self.on_disconnected += lambda _: setattr(self, '_idle_state', (None, None))

    def get_next_server(self):
        self._server_index = (self._server_index+1)%len(self.servers)
        return self.servers[self._server_index]

    def configure(self, runtime_environment):
        pass

    def buildProtocol(self, transport):
        return AMQProtocol(self.parameters)

    def _check_idle(self):
        if not self.protocol:
            # we're currently not connected anyway
            return

        state = self.protocol.bytes_sent, self.protocol.bytes_received

        if not state == self._idle_state:
            self._idle_state = state
            return

        if self.protocol.connection_state not in (connection.CONNECTION_CLOSED, connection.CONNECTION_CLOSING):
            self.protocol.close(320, 'Too long without data transferred.')

    def startService(self):
        if not self.running:
            service.MultiService.startService(self)

            self._keep_connecting()

    @defer.inlineCallbacks
    def stopService(self):
        if self.running:
            service.MultiService.stopService(self)

            if self.idle_checker.running:
                self.idle_checker.stop()

            yield self._disconnect('stopping service')

    @defer.inlineCallbacks
    def _keep_connecting(self):
        while self.running:
            try:
                self._connecting = self._connect()
                yield self._connecting

            except defer.CancelledError as ce:
                # the connection attempt might be cancelled, due to stopService/_disconnect
                break

            except (error.ConnectError, error.DNSLookupError, error.ConnectionDone) as ce:
                # ConnectionDone might be raised because we managed to connect, but reached the
                # max idle time during the AMQP handshake.
                log.info('Unable to connect: {0}'.format(ce))
                yield util.wait(1)
                continue

            except Exception as e:
                log.warn()
                yield util.wait(1)
                continue

            else:
                break

    @defer.inlineCallbacks
    def _connect(self):
        # make sure we're not directly overwriting an existing protocol
        if self.protocol:
            if self.protocol.connection_state not in (connection.CONNECTION_CLOSED, connection.CONNECTION_CLOSING):
                self._disconnect('reconnecting')

        server = self.get_next_server()
        endpoint = endpoints.clientFromString(reactor, server)

        log.info('Connecting to {0}'.format(server))
        
        self._connecting = endpoint.connect(self)

        # once twisted has connected to the other side, we set our protocol
        self.protocol = yield self._connecting

        # start the idle checker if necessary:
        if self.max_idle_time > 0:
            self._idle_state = (None, None)
            if self.idle_checker.running:
                self.idle_checker.stop()
            self.idle_checker.start(self.max_idle_time)
        
        yield self.protocol.ready

        self.on_connected(self.protocol)
        self.protocol.on_lost += self.on_disconnected
        self.protocol.on_lost += lambda reason: self._keep_connecting()

    def _disconnect(self, reason='unknown'):
        if self._connecting:
            self._connecting.cancel()

        if self.protocol:
            if not self.protocol.connection_state in (connection.CONNECTION_CLOSED, connection.CONNECTION_CLOSING):
                self.protocol.close()


class AMQPConsumerProvider(object, service.MultiService):
    """ Provides basic AMQP queue consumers. """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        self._connection_by_name = dict()
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)

        consumers = runtime_environment.get_configuration_value('amqp.basic_consumers', dict())

        for consumer_name, consumer_config in consumers.items():
            consumer = AMQPConsumer(name=consumer_name, **consumer_config)
            consumer.configure(runtime_environment)
            consumer.setServiceParent(self)


class AMQPConsumer(object, service.Service):
    current_deferred = None

    def __init__(self, name, pipeline, connection, queue=None, queue_declare=None, qos=None,
        ack_before_processing=False, ack_after_successful_processing=True,
        nack_after_failed_processing=True):
        self.name = name
        self.pipeline_name = pipeline
        self.connection_name = connection

        if queue and queue_declare:
            raise exceptions.ConfigurationError('Either use "queue" or "queue_declare", not both.')

        self.queue_name = queue
        self.queue_declaration = queue_declare

        self.qos = qos or dict()

        self.ack_before_processing = ack_before_processing
        self.ack_after_successful_processing = ack_after_successful_processing
        self.nack_after_failed_processing = nack_after_failed_processing

        if self.ack_before_processing and (self.ack_after_successful_processing or self.nack_after_failed_processing):
            raise Exception('cannot ack/nack before and after => double acking')

        self.workers = list()

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager

        self.pipeline_dependency = dm.add_dependency(self, dict(provider='pipeline.{0}'.format(self.pipeline_name)))

        self.connection_dependency = dm.add_dependency(self, dict(provider='amqp.connection.{0}'.format(self.connection_name)))
        self.connection_dependency.on_lost += lambda connection, reason: self._stop_working()
        self.connection_dependency.on_ready += lambda connection: self._ensure_working()

    def _currently(self, deferred):
        assert self.current_deferred is None

        if not isinstance(deferred, defer.Deferred):
            return deferred

        self.current_deferred = deferred
        def reset_current(result, deferred=deferred):
            if self.current_deferred == deferred:
                self.current_deferred = None
            return result

        return self.current_deferred.addBoth(reset_current)

    def _stop_working(self):
        if self.current_deferred:
            self.current_deferred.cancel()

    @defer.inlineCallbacks
    def _ensure_working(self):
        while self.running:
            if self.workers:
                return
            else:
                self.workers.append(self.run())
                try:
                    yield self.workers[0]
                except defer.CancelledError as ce:
                    # we might have been asked to stop by _stop_working:
                    return
                finally:
                    self.workers.pop()

    @defer.inlineCallbacks
    def run(self):
        currently = self._currently

        while self.running:
            try:
                connection = yield self.connection_dependency.wait_for_resource()
                channel = yield currently(connection.channel())

                if self.qos:
                    yield currently(channel.basic_qos(**self.qos))

                queue_name = self.queue_name

                if self.queue_declaration:
                    frame = yield currently(channel.queue_declare(**self.queue_declaration))
                    queue_name = frame.method.queue

                queue, consumer_tag = yield currently(channel.basic_consume(queue=queue_name))

                while self.running:
                    channel, method, properties, body = yield currently(queue.get())

                    if self.ack_before_processing:
                        yield currently(channel.basic_ack(delivery_tag=method.delivery_tag))

                    self._process(channel, method, properties, body)

            except pika_exceptions.ChannelClosed as cc:
                log.warn()
                yield util.wait(1)

            except Exception as e:
                log.warn()

    @defer.inlineCallbacks
    def _process(self, channel, method, properties, body):
        try:
            yield self.process(channel=channel, method=method, properties=properties, body=body)
        except Exception as e:
            log.warn()
            if self.nack_after_failed_processing:
                yield channel.basic_reject(delivery_tag=method.delivery_tag)
        else:
            if self.ack_after_successful_processing:
                yield channel.basic_ack(delivery_tag=method.delivery_tag)

    @defer.inlineCallbacks
    def process(self, **baton):
        pipeline = yield self.pipeline_dependency.wait_for_resource()
        yield pipeline.process(baton)

    def startService(self):
        if not self.running:
            service.Service.startService(self)
        self._ensure_working()

    def stopService(self):
        service.Service.stopService(self)


class RPCClientProvider(object, service.MultiService):
    """ Provides various RPC clients.

    Configuration example:

    .. code-block:: yaml

        amqp:
            clients:
                my_client:
                    type: piped.contrib.amqp.providers.RPCClient
                    exchange: gateway_operations
                    consumer:
                        queue_declare:
                            queue: ''
                            exclusive: true
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._client_by_name = dict()

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager

        self.clients = runtime_environment.get_configuration_value('amqp.clients', dict())
        resource_manager = runtime_environment.resource_manager

        for client_name, client_configuration in self.clients.items():
            resource_manager.register('amqp.client.%s' % client_name, provider=self)

            self._get_or_create_client(client_name)

    def _get_or_create_client(self, client_name):
        if client_name not in self._client_by_name:
            client_configuration = self.clients[client_name]
            type = client_configuration.pop('type', reflect.fullyQualifiedName(RPCClient))
            client = reflect.namedAny(type)(client_name, **client_configuration)
            client.configure(self.runtime_environment)
            client.setServiceParent(self)

            self._client_by_name[client_name] = client
        return self._client_by_name[client_name]


    def add_consumer(self, resource_dependency):
        client_name = resource_dependency.provider.rsplit('.', 1)[-1]
        client = self._get_or_create_client(client_name)
        client_dependency = self.dependency_manager.as_dependency(client)

        client_dependency.on_ready += lambda dep: resource_dependency.on_resource_ready(dep.get_resource())
        client_dependency.on_lost += lambda dep, reason: resource_dependency.on_resource_lost(reason)

        if client_dependency.is_ready:
            resource_dependency.on_resource_ready(client)


class RPCClient(object, service.Service):
    connection = None
    consume_channel = None
    response_queue = None

    _current_deferred = None

    def __init__(self, name, connection, exchange, queue_declare=None, timeout=10):
        self.name = name
        self.connection_name = connection
        self.exchange = exchange

        self.queue_declare = queue_declare or dict()
        self.queue_declare.setdefault('queue', '')
        self.queue_declare.setdefault('auto_delete', True)
        self.queue_declare.setdefault('durable', False)
        self.queue_declare.setdefault('exclusive', True)

        self.timeout = timeout

        self.requests = dict()
        self.on_connection_lost = event.Event()
        self.on_connection_lost += lambda reason: self._handle_disconnect()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = self.runtime_environment.dependency_manager
        dm = self.dependency_manager

        self.dependency = dm.as_dependency(self)
        self.dependency.cascade_ready = False
        self.dependency.on_dependency_ready += lambda dependency: self._consider_starting()

        self.connection_dependency = dm.add_dependency(self, dict(provider='amqp.connection.{0}'.format(self.connection_name)))
        self.connection_dependency.on_lost += lambda dep, reason: self.on_connection_lost(reason)

    def _currently(self, deferred):
        assert self._current_deferred is None

        if not isinstance(deferred, defer.Deferred):
            return deferred

        self._current_deferred = deferred
        def reset_current(result, deferred=deferred):
            assert self._current_deferred == deferred
            self._current_deferred = None
            return result

        return self._current_deferred.addBoth(reset_current)

    @defer.inlineCallbacks
    def _consider_starting(self):
        if not self.dependency_manager.has_all_dependencies_provided(self.dependency):
            return

        currently = self._currently
        try:
            if not self.connection:
                self.connection = yield currently(self.connection_dependency.wait_for_resource())
            if not self.consume_channel:
                self.consume_channel = yield currently(self.connection.channel())
            if not self.response_queue:
                self.response_queue = yield currently(self.consume_channel.queue_declare(**self.queue_declare))

            self._consume_queue()
        except defer.CancelledError as ce:
            log.info('Initialization of %r was cancelled.' % self)

    def _handle_disconnect(self):
        self.connection = None
        self.consume_channel = None
        self.response_queue = None

        # cancel any current operations
        if self._current_deferred:
            self._current_deferred.cancel()

    def startService(self):
        service.Service.startService(self)
        self._consider_starting()

    def stopService(self):
        service.Service.stopService(self)
        self.dependency.fire_on_lost('stopping client')

    @defer.inlineCallbacks
    def _consume_queue(self):
        currently = self._currently
        queue, consumer_tag = yield currently(self.consume_channel.basic_consume(queue=self.response_queue.method.queue, no_ack=True, exclusive=True))
        if self.running:
            self.dependency.fire_on_ready()
        try:
            while self.dependency.is_ready and self.running:
                channel, method, properties, body = yield currently(queue.get())
                d = defer.maybeDeferred(self.process_response, channel, method, properties, body)
        except defer.CancelledError as ce:
            return

    def process_request(self, *a, **kw):
        raise NotImplementedError()

    def process_response(self, channel, method, properties, body):
        raise NotImplementedError()