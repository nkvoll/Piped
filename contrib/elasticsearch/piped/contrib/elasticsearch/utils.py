# -*- coding: utf-8 -*-
from piped import util


__author__ = 'Alberto Paro'
__all__ = ['clean_string', "ESRange", "ESRangeOp", "string_b64encode", "string_b64decode"]
import base64
import json

from datetime import date, datetime
import time
from decimal import Decimal


def string_b64encode(s):
    """
    This function is useful to convert a string to a valid id to be used in ES.
    You can use it to generate an ID for urls or some texts
    """
    return base64.urlsafe_b64encode(s).strip('=')

def string_b64decode(s):
    return base64.urlsafe_b64decode(s + '=' * (len(s) % 4))

# Characters that are part of Lucene query syntax must be stripped
# from user input: + - && || ! ( ) { } [ ] ^ " ~ * ? : \
# See: http://lucene.apache.org/java/3_0_2/queryparsersyntax.html#Escaping
SPECIAL_CHARS = [33, 34, 38, 40, 41, 42, 45, 58, 63, 91, 92, 93, 94, 123, 124, 125, 126]
UNI_SPECIAL_CHARS = dict((c, None) for c in SPECIAL_CHARS)
STR_SPECIAL_CHARS = ''.join([chr(c) for c in SPECIAL_CHARS])

class ESRange(object):
    def __init__(self, to=None, include_lower=None,
                 include_upper=None, boost=None, **kwargs):
        """
        type can be "gt", "gte", "lt", "lte"

        """
        self.from_ = kwargs.pop('from')
        self.to = to
        self.include_lower = include_lower
        self.include_upper = include_upper
        self.boost = boost

    def serialize(self):

        filters = {}
        if self.from_ is not None:
            filters['from'] = self.from_
        if self.to is not None:
            filters['to'] = self.to
        if self.include_lower is not None:
            filters['include_lower'] = self.include_lower
        if self.include_upper is not None:
            filters['include_upper'] = self.include_upper
        if self.boost is not None:
            filters['boost'] = self.boost
        return filters

    @classmethod
    def unserialize(cls, data):
        return cls(**data)

class ESRangeOp(ESRange):
    def __init__(self, field, op, value, boost=None):
        from_value = to_value = include_lower = include_upper = None
        if op == "gt":
            from_value = value
            include_lower = False
        elif op == "gte":
            from_value = value
            include_lower = True
        if op == "lt":
            to_value = value
            include_upper = False
        elif op == "lte":
            to_value = value
            include_upper = True
        super(ESRangeOp, self).__init__(field, from_value, to_value,
                include_lower, include_upper, boost)

def clean_string(text):
    """
    Remove Lucene reserved characters from query string
    """
    if isinstance(text, unicode):
        return text.translate(UNI_SPECIAL_CHARS).strip()
    return text.translate(None, STR_SPECIAL_CHARS).strip()

def keys_to_string(data):
    """
    Function to convert all the unicode keys in string keys
    """
    if isinstance(data, dict):
        for key in list(data.keys()):
            if isinstance(key, unicode):
                value = data[key]
                val = keys_to_string(value)
                del data[key]
                data[key.encode("utf8", "ignore")] = val
    return data


class ESJsonEncoder(json.JSONEncoder):
    def default(self, value):
        """Convert rogue and mysterious data types.
        Conversion notes:

        - ``datetime.date`` and ``datetime.datetime`` objects are
        converted into datetime strings.
        """

        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, 0, 0, 0)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(value, Decimal):
            return float(str(value))
        else:
            # use no special encoding and hope for the best
            return value


class ESJsonDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        kwargs['object_hook'] = self.dict_to_object
        json.JSONDecoder.__init__(self, *args, **kwargs)

    def string_to_datetime(self, obj):
        """Decode a datetime string to a datetime object
        """
        if isinstance(obj, basestring) and len(obj) == 19:
            try:
                return datetime(*obj.strptime("%Y-%m-%dT%H:%M:%S")[:6])
            except Exception:
                pass
        return obj

    def dict_to_object(self, d):
        """
        Decode datetime value from string to datetime
        """
        for k, v in d.items():
            if isinstance(v, basestring) and len(v) == 19:
                try:
                    d[k] = datetime(*time.strptime(v, "%Y-%m-%dT%H:%M:%S")[:6])
                except ValueError:
                    pass
            elif isinstance(v, list):
                d[k] = [self.string_to_datetime(elem) for elem in v]
        return util.AttributeDict(d)


def decode_json(data):
    """ Decode some json to dict"""
    return json.loads(data, cls=ESJsonDecoder)

def encode_json(data):
    """ Encode some json to dict"""
    return json.dumps(data, cls=ESJsonEncoder)