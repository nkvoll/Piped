# Copyright (c) 2010-2011, Found IT A/S and Found Project Contributors.
# All rights reserved.
#
# This module is part of Found and is released under
# the BSD License: http://www.opensource.org/licenses/bsd-license.php
import copy
import collections
import xml.etree.cElementTree as etree

from twisted.internet import defer
from zope import interface
from txsolr import client

from piped import processing, util, yamlutil, exceptions
from piped.processors import base


class SolrBinder(base.Processor):
    """ Adds a Solr client in the baton.

    This processor is typically used as the first solr processor in a pipeline, giving the rest
    of the processors access to a solr server.
    """
    interface.classProvides(processing.IProcessor)
    name = 'bind-solr-client'

    client_factory = client.SolrClient
    _client_by_factory_and_url = collections.defaultdict(dict)

    def __init__(self, url, output_path='solr_client', *a, **kw):
        """
        :param url: The url to the solr server endpoint. Example: ``http://localhost:8983/solr``. A
            :ref:`path-constructor` may be used to denote that the url is found in the baton.
        :param output_path: The path in the baton to set the client.
        """
        super(SolrBinder, self).__init__(*a, **kw)

        self.url = url
        self.output_path = output_path

    def process(self, baton):
        url = self.get_input(baton, self.url)
        solr_client = self._get_solr_client_by_url(url)
        util.dict_set_path(baton, self.output_path, solr_client)
        return baton

    def _get_solr_client_by_url(self, url):
        client_by_url = self._client_by_factory_and_url[self.client_factory]
        if not url in client_by_url:
            solr_client = self.client_factory(url)
            client_by_url[url] = solr_client

        return client_by_url[url]


class SolrProcessor(base.Processor):
    """ A base class for Solr processors. """
    
    def __init__(self, client='solr_client', **kw):
        """
        :param client: The path to the client in the baton.
        """
        super(SolrProcessor, self).__init__(**kw)
        self.client = client

    def get_client(self, baton):
        return util.dict_get_path(baton, self.client)


class AddSolr(SolrProcessor):
    """ Add or update documents in Solr. """
    interface.classProvides(processing.IProcessor)
    name = 'add-solr'

    def __init__(self, documents, output_path=None, overwrite=None, commit_within=None, **kw):
        """
        :param documents: A dict or list of dicts representing the documents. The keys should be field names and the values
            field content.
        :param overwrite: Whether newer documents will replace previously added documents with the same uniqueKey.
        :param commit_within: (milliseconds) The document will be added within this time.
        :param output_path: Where the output from the request should be stored in the baton.
        """
        super(AddSolr, self).__init__(**kw)

        self.documents = documents
        self.output_path = output_path
        self.overwrite = overwrite
        self.commit_within = commit_within

    @defer.inlineCallbacks
    def process(self, baton):
        client = yield self.get_client(baton)
        documents = self.get_input(baton, self.documents)
        response = yield client.add(documents, overwrite=self.overwrite, commitWithin=self.commit_within)

        baton = self.get_resulting_baton(baton, self.output_path, response.responseDict)
        defer.returnValue(baton)


class CommitSolr(SolrProcessor):
    """ Commit a Solr index. """
    interface.classProvides(processing.IProcessor)
    name = 'commit-solr'

    def __init__(self, wait_searcher=None, expunge_deletes=None, **kw):
        """
        :param wait_searcher: Block until a new searcher is opened and registered as the main query searcher,
            making the changes visible.
        :param expunge_deletes: Merge segments with deletes away.
        """
        super(CommitSolr, self).__init__(**kw)

        self.kwargs = dict(waitSearcher=wait_searcher, expungeDeletes=expunge_deletes)

    @defer.inlineCallbacks
    def process(self, baton):
        client = yield self.get_client(baton)
        yield client.commit(**self.kwargs)
        defer.returnValue(baton)


class DeleteSolr(SolrProcessor):
    """ Delete documents from a Solr index. """
    interface.classProvides(processing.IProcessor)
    name = 'delete-solr'

    def __init__(self, ids=None, query=None, output_path=None, **kw):
        """
        :param ids: A string or a list of strings representing the ids of the documents to delete.
        :param query: A Solr query that returns the document to be deleted.
        :param output_path: Where the output from the delete request should be stored.
        """
        super(DeleteSolr, self).__init__(**kw)

        self.ids = ids
        self.query = query
        self.output_path = output_path

        if (ids is None) + (query is None) != 1:
            e_msg = 'Invalid processor configuration.'
            detail = 'Either specify "ids" or "query" to this processor.'
            raise exceptions.ConfigurationError(e_msg, detail)

    @defer.inlineCallbacks
    def process(self, baton):
        client = yield self.get_client(baton)
        output = Ellipsis

        if self.ids:
            ids = self.get_input(baton, self.ids)
            if ids is not Ellipsis:
                output = yield client.delete(ids)

        if self.query:
            query = self.get_input(baton, self.query)
            if query is not Ellipsis:
                output = yield client.deleteByQuery(query)

        if output is not Ellipsis:
            baton = self.get_resulting_baton(baton, self.output_path, output)

        defer.returnValue(baton)


class RollbackSolr(SolrProcessor):
    """ Rollback a Solr index. """
    interface.classProvides(processing.IProcessor)
    name = 'rollback-solr'

    @defer.inlineCallbacks
    def process(self, baton):
        client = yield self.get_client(baton)
        yield client.rollback()
        defer.returnValue(baton)


class OptimizeSolr(SolrProcessor):
    """ Optimize a Solr index. """
    interface.classProvides(processing.IProcessor)
    name = 'optimize-solr'

    def __init__(self, wait_searcher=None, max_segments=None, **kw):
        """
        :param wait_searcher: Block until a new searcher is opened and registered as the main query searcher,
            making the changes visible.
        :param max_segments: Optimize down to at most this number of segments.
        """
        super(OptimizeSolr, self).__init__(**kw)

        self.kwargs = dict(waitSearcher=wait_searcher, maxSegments=max_segments)

    @defer.inlineCallbacks
    def process(self, baton):
        client = yield self.get_client(baton)
        yield client.optimize(**self.kwargs)
        defer.returnValue(baton)


class SearchSolr(SolrProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'search-solr'

    def __init__(self, query=yamlutil.BatonPath('query.q'), query_params=yamlutil.BatonPath('query'), defaults=None, output_path='query_results', **kw):
        """
        :param query: The query to run.

            .. seealso:: http://wiki.apache.org/solr/SolrQuerySyntax

        :param query_params: Additional parameters to the server.

            .. seealso::

                * Common parameters: http://wiki.apache.org/solr/CommonQueryParameters
                * Highlighting: http://wiki.apache.org/solr/HighlightingParameters
                * Dismax: http://wiki.apache.org/solr/DisMaxQParserPlugin.

        :param defaults: A dict containing defaults for the query parameters.
        :param output_path: Where the output from the request should be stored.
        """
        super(SearchSolr, self).__init__(**kw)

        self.query = query
        self.query_params = query_params
        self.defaults = defaults or dict()
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        client = yield self.get_client(baton)

        query_string = self.get_input(baton, self.query)
        query_params = self.get_input(baton, self.query_params)

        # construct the query, we deepcopy so that we dont modify the originals
        query_kwargs = copy.deepcopy(self.defaults)
        query_kwargs.update(copy.deepcopy(query_params))

        self._flatten_query_params(query_kwargs)

        response = yield client.search(query_string, **query_kwargs)

        baton = self.get_resulting_baton(baton, self.output_path, response.responseDict)
        defer.returnValue(baton)

    def _flatten_query_params(self, query_params):
        for path, value in list(util.dict_iterate_paths(query_params)):
            if isinstance(value, dict):
                # if it is a dict, we remove the element completely. the contents
                # of the dict(if any) will be added in later iterations.
                util.dict_remove_path(query_params, path)
                continue

            if isinstance(value, bool):
                # boolean values are in lowercase in Solr
                value = str(value).lower()
            elif isinstance(value, (list, tuple)):
                # lists are usually comma-separated
                value = ','.join(value)

            query_params[path] = value


class UpdateXMLParser(base.InputOutputProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'parse-solr-update-xml'

    def process_input(self, input, baton):
        root = etree.fromstring(input)
        result = dict(
            adds=self._get_adds(root),
            deletes=self._get_deletes(root),
            commit=self._get_commit(root),
            optimize=self._get_optimize(root)
        )
        for key, value in result.items():
            if value == list() or value is None:
                del result[key]
        return result

    def _get_all(self, root, tag_name):
        if root.tag == tag_name:
            return [root]

        return root.findall(tag_name)

    def _get_adds(self, root):
        result = []

        for add_element in self._get_all(root, 'add'):
            docs = list()

            for doc_element in add_element.findall('doc'):
                fields = dict()
                boosts = dict()

                for field_element in doc_element.findall('field'):
                    field_name = field_element.get('name')
                    field = fields[field_name] = field_element.text
                    boost = field_element.get('boost')
                    if boost:
                        boosts[field_name] = boost

                doc = dict(fields=fields)
                if boosts:
                    doc['field_boosts'] = boosts

                doc_boost = doc_element.get('boost')
                if doc_boost:
                    doc['document_boost'] = doc_boost
                docs.append(doc)

            result.append(dict(add_element.items(), docs=docs))

        return result

    def _get_deletes(self, root):
        result = []
        for delete_element in self._get_all(root, 'delete'):
            result.append(dict(delete_element.items(),
                               ids=[el.text for el in delete_element.findall('id')],
                               queries=[el.text for el in delete_element.findall('query')]))
        return result

    def _get_commit(self, root):
        commit_elements = self._get_all(root, 'commit')
        if not commit_elements:
            return

        commit = dict()
        for el in commit_elements:
            commit.update(el.items())
        return commit

    def _get_optimize(self, root):
        optimize_elements = self._get_all(root, 'optimize')
        if not optimize_elements:
            return

        optimize = dict()
        for el in optimize_elements:
            optimize.update(el.items())
        return optimize
