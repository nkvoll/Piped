# Copyright (c) 2010-2011, Found IT A/S and Found Project Contributors.
# All rights reserved.
#
# This module is part of Found and is released under
# the BSD License: http://www.opensource.org/licenses/bsd-license.php
import json

import pika
from twisted.internet import defer
from zope import interface

from piped import processing, util, yamlutil, exceptions
from piped.processors import base



class BasicPublish(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'amqp-basic-publish'

    def __init__(self, exchange='', routing_key=yamlutil.BatonPath('method.routing_key'),
                body=yamlutil.BatonPath('body'), properties=yamlutil.BatonPath('properties'),
                channel=yamlutil.BatonPath('channel'), ack_delivery_tag=None, *a, **kw):
        super(BasicPublish, self).__init__(*a, **kw)

        self.exchange = exchange
        self.routing_key = routing_key

        self.body = body
        self.properties = properties
        self.channel = channel

        self.ack_delivery_tag = ack_delivery_tag

    @defer.inlineCallbacks
    def process(self, baton):
        channel = self.get_input(baton, self.channel)

        exchange = self.get_input(baton, self.exchange)
        routing_key = self.get_input(baton, self.routing_key)

        body = self.get_input(baton, self.body)
        properties = self.get_input(baton, self.properties) or pika.BasicProperties()

        yield channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)

        if self.ack_delivery_tag:
            delivery_tag = self.get_input(baton, self.ack_delivery_tag)
            yield channel.ack(delivery_tag=delivery_tag)


class BasicRPCReply(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'amqp-rpc-reply'

    def __init__(self, exchange='', routing_key=yamlutil.BatonPath('properties.reply_to'),
                body=yamlutil.BatonPath('body'), correlation_id=yamlutil.BatonPath('properties.correlation_id'),
                channel=yamlutil.BatonPath('channel'), ack_delivery_tag=None, *a, **kw):
        super(BasicRPCReply, self).__init__(*a, **kw)

        self.exchange = exchange
        self.routing_key = routing_key

        self.body = body
        self.correlation_id = correlation_id
        self.channel = channel

        self.ack_delivery_tag = ack_delivery_tag

    @defer.inlineCallbacks
    def process(self, baton):
        channel = self.get_input(baton, self.channel)

        exchange = self.get_input(baton, self.exchange)
        routing_key = self.get_input(baton, self.routing_key)
        correlation_id = self.get_input(baton, self.correlation_id)

        body = self.get_input(baton, self.body)
        properties = pika.BasicProperties(correlation_id=correlation_id)

        yield channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)

        if self.ack_delivery_tag:
            delivery_tag = self.get_input(baton, self.ack_delivery_tag)
            yield channel.ack(delivery_tag=delivery_tag)


class RPCForwarder(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'forward-amqp-rpc'

    def __init__(self, exchange, routing_key=yamlutil.BatonPath('method.routing_key'),
                body=yamlutil.BatonPath('body'), properties=yamlutil.BatonPath('properties'),
                channel=yamlutil.BatonPath('channel'), ack_delivery_tag=None, *a, **kw):
        super(RPCForwarder, self).__init__(*a, **kw)

        self.exchange = exchange
        self.routing_key = routing_key

        self.body = body
        self.properties = properties
        self.channel = channel

        self.ack_delivery_tag = ack_delivery_tag

    @defer.inlineCallbacks
    def process(self, baton):
        channel = self.get_input(baton, self.channel)
        exchange = self.get_input(baton, self.exchange)
        routing_key = self.get_input(baton, self.routing_key)

        body = self.get_input(baton, self.body)
        properties = self.get_input(baton, self.properties)

        yield channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)

        if self.ack_delivery_tag:
            delivery_tag = self.get_input(baton, self.ack_delivery_tag)
            yield channel.ack(delivery_tag=delivery_tag)
