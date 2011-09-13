# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from zope import interface
from twisted.python import reflect

from piped import util, processing
from piped.processors import base, json_processors

from piped.contrib.elasticsearch import query
from piped.contrib.elasticsearch import utils


class ParseSearch(base.InputOutputProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'parse-es-search'

    def __init__(self, **kw):
        super(ParseSearch, self).__init__(**kw)

    def process_input(self, input, baton):
        return query.Search.unserialize(input)


class SerializeSearch(base.InputOutputProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'serialize-es-search'

    def __init__(self, **kw):
        super(SerializeSearch, self).__init__(**kw)

    def process_input(self, input, baton):
        return input.q


class JsonDecoder(json_processors.JsonDecoder):
    interface.classProvides(processing.IProcessor)
    name = 'decode-es-json'

    def __init__(self, decoder=reflect.fullyQualifiedName(utils.ESJsonDecoder), **kw):
        super(JsonDecoder, self).__init__(decoder=decoder, **kw)


class JsonEncoder(json_processors.JsonEncoder):
    interface.classProvides(processing.IProcessor)
    name = 'encode-es-json'

    def __init__(self, encoder=reflect.fullyQualifiedName(utils.ESJsonEncoder), **kw):
        super(JsonEncoder, self).__init__(encoder=encoder, **kw)


class JSONPEncoder(json_processors.JSONPEncoder):
    name = 'encode-es-jsonp'
    interface.classProvides(processing.IProcessor)

    def __init__(self, encoder=reflect.fullyQualifiedName(utils.ESJsonEncoder), **kw):
        super(JSONPEncoder, self).__init__(encoder=encoder, **kw)