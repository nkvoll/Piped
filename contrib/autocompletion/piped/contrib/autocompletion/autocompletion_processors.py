# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
import json
import urlparse
import urllib
import urllib2

from twisted.internet import reactor, defer, interfaces, threads
from zope import interface

import autocompleter
from piped import util, processing
from piped.processors import base


class Autocompleter(base.InputOutputProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'autocomplete'

    def __init__(self, file_path, sentences=False, k=4, expand_if_not_enough=True, expand_min_length=5, **kw):
        super(Autocompleter, self).__init__(**kw)
        self.autocompleter = autocompleter.AutoCompleter(file_path)
        self.k = k
        self.expand_if_not_enough = expand_if_not_enough
        self.expand_min_length = expand_min_length

        if sentences:
            self._autocomplete = self.autocompleter.query_for_matching_strings
        else:
            self._autocomplete = self.autocompleter.query_for_ids

    def process_input(self, input, baton):
        return threads.deferToThread(self._get_autocompletions, input)

    def _get_autocompletions(self, query):
        length = len(query)
        query = query.encode('utf8')
        results = self._autocomplete(self.autocompleter.delimiter + query)

        ids = set(result[0] for result in results)

        if len(results) < self.k and self.expand_if_not_enough and length >= self.expand_min_length:
            additional_results = self._autocomplete(' ' + query)
            for result in additional_results:
                if result[0] not in ids:
                    results.append(result)

        return results

    
