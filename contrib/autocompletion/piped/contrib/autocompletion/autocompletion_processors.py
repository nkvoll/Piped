# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
import json
import urlparse
import urllib
import urllib2

from twisted.internet import reactor, defer, interfaces, threads
from zope import interface

import autocompletion
from piped import util, processing
from piped.processors import base


class Autocompleter(base.InputOutputProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'autocomplete'

    def __init__(self, file_path, sentences=False, k=4, substring_min_length=5, **kw):
        super(Autocompleter, self).__init__(**kw)
        self.autocompleter = autocompletion.AutoCompleter(file_path)
        self.k = k
        self.substring_min_length = substring_min_length

        if sentences:
            self._autocomplete = self.autocompleter.query_for_matching_strings
        else:
            self._autocomplete = self.autocompleter.query_for_ids

    def process_input(self, input, baton):
        return threads.deferToThread(self._get_autocompletions, input)

    def _get_autocompletions(self, query):
        length = len(query)
        k = self.k
        query = query.encode('utf8')

        queries = [self.autocompleter.delimiter + query, ' ' + query]
        if length >= self.substring_min_length:
            queries.append(query)

        results = list()
        already_included = set()
        for query in queries:
            hits = self._autocomplete(query, k)
            for hit in hits:
                if hit in already_included:
                    continue
                already_included.add(hit)
                if len(results) < k:
                    results.append(hit)
                else:
                    break
            if len(results) == k:
                break

        return results
