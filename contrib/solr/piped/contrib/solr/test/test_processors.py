# Copyright (c) 2010-2011, Found IT A/S and Found Project Contributors.
# All rights reserved.
#
# This module is part of Found and is released under
# the BSD License: http://www.opensource.org/licenses/bsd-license.php
import json

import mock
from twisted.internet import defer
from twisted.trial import unittest
from txsolr import client, response

from piped import yamlutil, util, exceptions
from piped.contrib.solr import processors


class StubSolrProcessor(processors.SolrProcessor):
    name = 'stub-processor'

    def process(self, baton):
        return baton


class TestSolrBinder(unittest.TestCase):

    def _create_processor(self, **config):
        processor = processors.SolrBinder(**config)
        return processor

    def test_simple_binding(self):
        processor = self._create_processor(url='http://foo/bar')
        baton = processor.process(dict())

        self.assertIsInstance(baton['solr_client'], client.SolrClient)
        self.assertEquals(baton['solr_client'].url, 'http://foo/bar')

    def test_binding_url_in_baton(self):
        processor = self._create_processor(url=yamlutil.BatonPath('url'))
        baton = processor.process(dict(url='http://foo/bar'))

        self.assertIsInstance(baton['solr_client'], client.SolrClient)
        self.assertEquals(baton['solr_client'].url, 'http://foo/bar')

    def test_only_creating_one_client_per_url(self):
        # the same processor should reuse the same client when processing multiple batons.
        processor_1 = self._create_processor(url=yamlutil.BatonPath('url'))
        baton_1 = processor_1.process(dict(url='http://foo/bar'))
        baton_2 = processor_1.process(dict(url='http://foo/bar'))

        # the client should be shared across processors
        processor_2 = self._create_processor(url=yamlutil.BatonPath('url'))
        baton_3 = processor_2.process(dict(url='http://foo/bar'))

        self.assertEquals(baton_1['solr_client'], baton_2['solr_client'])
        self.assertEquals(baton_2['solr_client'], baton_3['solr_client'])

        # distinct urls should use distinct clients, in the same processor...
        baton_4 = processor_2.process(dict(url='http://bar/foo'))
        self.assertNotEquals(baton_4['solr_client'], baton_3['solr_client'])

        # .. and in different processors:
        processor_3 = self._create_processor(url=yamlutil.BatonPath('url'))
        baton_5 = processor_3.process(dict(url='http://bar/foo'))
        self.assertNotEquals(baton_5['solr_client'], baton_3['solr_client'])


class SolrBaseProcessorTest(unittest.TestCase):

    def test_get_client(self):
        processor = StubSolrProcessor()
        client = processor.get_client(dict(solr_client='fake_client'))
        self.assertEquals(client, 'fake_client')


class SolrProcessorTest(unittest.TestCase):
    processor_class = processors.SolrProcessor

    def _create_processor(self, **config):
        processor = self.processor_class(**config)
        return processor

    @defer.inlineCallbacks
    def assertClientRequests(self, processor, baton, body, request_response):
        solr_client = client.SolrClient('fake_url')
        
        with mock.patch.object(processor, 'get_client') as mocked_get_client:
            mocked_get_client.return_value = solr_client

            with mock.patch.object(solr_client, '_request') as mocked_request:
                def mocked_request_side_effect(method, path, headers, body_producer):
                    if not body_producer:
                        input = path.split('?', 1)[-1]
                    else:
                        input = body_producer.body

                    self.assertEquals(input, body)
                    result = response.JSONSolrResponse(json.dumps(request_response))
                    return defer.succeed(result)

                mocked_request.side_effect = mocked_request_side_effect

                result = yield processor.process(baton)
                self.assertEquals(mocked_request.call_count, 1)


class TestAdd(SolrProcessorTest):
    processor_class = processors.AddSolr

    @defer.inlineCallbacks
    def test_simple_update(self):
        processor = self._create_processor(documents=yamlutil.BatonPath('document'), output_path='response')

        baton = dict(document=dict(foo='bar'))

        update_response =  dict(
            responseHeader = dict(status=0),
            response = dict(
                numFound = 2,
                start = 0,
                docs = ['one', 'two']
            )
        )

        expected_body = '<add><doc><field name="foo">bar</field></doc></add>'

        yield self.assertClientRequests(processor, baton, expected_body, update_response)
        self.assertEquals(baton['response'], update_response)


class TestCommit(SolrProcessorTest):
    processor_class = processors.CommitSolr

    @defer.inlineCallbacks
    def test_simple_commit(self):
        processor = self._create_processor()
        baton = dict()
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = '<commit />'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)


class TestDelete(SolrProcessorTest):
    processor_class = processors.DeleteSolr

    @defer.inlineCallbacks
    def test_simple_delete(self):
        processor = self._create_processor(ids=[1,2,3])
        baton = dict()
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = '<delete><id>1</id><id>2</id><id>3</id></delete>'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)

    @defer.inlineCallbacks
    def test_delete_by_query(self):
        processor = self._create_processor(query='id:*')
        baton = dict()
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = '<delete><query>id:*</query></delete>'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)

    def test_invalid_configurations(self):
        self.assertRaises(exceptions.ConfigurationError, self._create_processor, query='id:*', ids=[])
        self.assertRaises(exceptions.ConfigurationError, self._create_processor)


class TestRollback(SolrProcessorTest):
    processor_class = processors.RollbackSolr

    @defer.inlineCallbacks
    def test_simple_rollback(self):
        processor = self._create_processor()
        baton = dict()
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = '<rollback />'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)


class TestOptimize(SolrProcessorTest):
    processor_class = processors.OptimizeSolr

    @defer.inlineCallbacks
    def test_simple_optimize(self):
        processor = self._create_processor()
        baton = dict()
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = '<optimize />'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)


class TestSearch(SolrProcessorTest, unittest.TestCase):
    processor_class = processors.SearchSolr

    @defer.inlineCallbacks
    def test_simple_search(self):
        processor = self._create_processor()
        baton = dict(query=dict(q='foo', qf='id'))
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = 'q=foo&wt=json&qf=id'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)

    @defer.inlineCallbacks
    def test_defaults(self):
        processor = self._create_processor(defaults=dict(qf='id^20'))
        baton = dict(query=dict(q='foo'))
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = 'q=foo&wt=json&qf=id%5E20'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)

    @defer.inlineCallbacks
    def test_flattening(self):
        # lists should be joined with a comma
        # underscores in keys should be rewritten as dots.
        # boolean values should be lowercased
        processor = self._create_processor(defaults=dict(hl=True, hl_fl=['foo', 'bar']))
        baton = dict(query=dict(q='foo'))
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = 'q=foo&hl.fl=foo%2Cbar&wt=json&hl=true'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)

    @defer.inlineCallbacks
    def test_flattening_nested_dicts(self):
        # lists should be joined with a comma
        # underscores in keys should be rewritten as dots.
        # boolean values should be lowercased
        processor = self._create_processor(defaults=dict(hl=True, hl_fl='fieldName'))
        baton = dict(query=dict(q='foo', f=dict(fieldName=dict(hl=dict(snippets=2)))))
        request_response =  dict(responseHeader = dict(status=0))
        expected_body = 'q=foo&f.fieldName.hl.snippets=2&wt=json&hl=true&hl.fl=fieldName'
        yield self.assertClientRequests(processor, baton, expected_body, request_response)


class TestUpdateXMLParser(unittest.TestCase):

    def get_resulting_dict(self, xml):
        return processors.UpdateXMLParser().process_input(xml, dict())

    def assertXMLResultsIn(self, xml, expected_result, msg=None):
        self.assertEquals(self.get_resulting_dict(xml), expected_result, msg)

    def test_simple_add(self):
        xml = """<add><doc>
            <field name="employeeId">05991</field>
            <field name="office">Bridgewater</field>
            </doc></add>
        """
        expected_result = dict(
            adds=[
                dict(
                    docs=[
                        dict(
                            fields=dict(
                                employeeId='05991',
                                office='Bridgewater'
                                )
                            )
                        ]
                    )
                ]
            )

        self.assertXMLResultsIn(xml, expected_result)

    def test_simple_delete(self):
        xml = '<delete><id>42</id><id>123</id></delete>'
        expected_result = dict(deletes=[dict(ids=['42', '123'], queries=[])])
        self.assertXMLResultsIn(xml, expected_result)

    def test_simple_delete_by_query(self):
        xml = '<delete><query>foo</query></delete>'
        expected_result = dict(deletes=[dict(ids=[], queries=['foo'])])
        self.assertXMLResultsIn(xml, expected_result)

    def test_delete_and_commit(self):
        xml = '<update><delete><id>42</id></delete><commit /></update>'
        expected_result = dict(deletes=[dict(ids=['42'], queries=[])], commit=dict())
        self.assertXMLResultsIn(xml, expected_result)

    def test_commit_and_optimize_with_options(self):
        xml = '<update><commit foo="bar" /><optimize bar="baz" /></update>'
        expected_result = dict(commit=dict(foo="bar"), optimize=dict(bar="baz"))
        self.assertXMLResultsIn(xml, expected_result)

    def test_parsing_update_document_with_a_bit_of_everyting(self):
        xml = """
        <update>
          <add overwrite="true">
            <doc boost="2.5">
              <field name="employeeId">05991</field>
              <field name="office" boost="2.0">Bridgewater</field>
            </doc>
            <doc>
              <field name="employeeId">05991</field>
              <field name="office" boost="2.0">Waterbridge</field>
            </doc>

          </add>
          <delete>
            <id>05991</id>
          </delete>
          <delete fromPending="true">
            <query>office:Bridgewater</query>
          </delete>
          <commit expungeDeletes="true" />
          <optimize maxSegments="42"/>
        </update>
        """

        expected_result = {
            'adds': [
                {'docs': [
                        {'document_boost': '2.5',
                         'field_boosts': {'office': '2.0'},
                         'fields': {'employeeId': '05991',
                                    'office': 'Bridgewater'}},
                        {'fields': {'employeeId': '05991',
                                    'office': 'Waterbridge'},
                         'field_boosts': {'office': '2.0'}}],
                 'overwrite': 'true'}],
            'commit': {'expungeDeletes': 'true'},
            'deletes': [{'ids': ['05991'], 'queries': []},
                        {'fromPending': 'true',
                         'ids': [],
                         'queries': ['office:Bridgewater']}],
            'optimize': {'maxSegments': '42'}}

        self.assertXMLResultsIn(xml, expected_result)
