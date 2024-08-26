# -*- coding: utf8 -*-

import unittest
from tests.lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging
import json

class SearchIndexTest(APITestBase):

    def _check_field_schema(self, expect_field_schema, actual_field_schema):
        self.assert_equal(expect_field_schema.field_name, actual_field_schema.field_name)
        self.assert_equal(expect_field_schema.field_type, actual_field_schema.field_type)

        self.assert_equal(expect_field_schema.index if expect_field_schema.index else True,
                            actual_field_schema.index)
        self.assert_equal(expect_field_schema.is_array if expect_field_schema.is_array else False,
                            actual_field_schema.is_array)
        self.assert_equal(expect_field_schema.enable_sort_and_agg if expect_field_schema.enable_sort_and_agg else False,
                            actual_field_schema.enable_sort_and_agg)

        if actual_field_schema.field_type == FieldType.TEXT:
            self.assert_equal(expect_field_schema.analyzer if expect_field_schema.analyzer is not None else AnalyzerType.SINGLEWORD,
                                actual_field_schema.analyzer)

        if expect_field_schema.enable_highlighting is not None:
            self.assert_equal(expect_field_schema.enable_highlighting, actual_field_schema.enable_highlighting)

    def _check_index_meta(self, expect_index_meta, actual_index_meta):
        self.assert_equal(len(expect_index_meta.fields), len(actual_index_meta.fields))
        for i in range(len(actual_index_meta.fields)):
            self._check_field_schema(expect_index_meta.fields[i], actual_index_meta.fields[i])

        # check index setting
        if expect_index_meta.index_setting is None:
            self.assert_equal(actual_index_meta.index_setting.routing_fields, [])
        else:
            self.assert_equal(actual_index_meta.index_setting.routing_fields, expect_index_meta.index_setting.routing_fields)

    def test_create_search_index(self):
        table_name = 'full_text_search_test_' + self.get_python_version()
        table_meta = TableMeta(table_name, [('url', 'STRING')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        field_a = FieldSchema('type', FieldType.KEYWORD, index=True, enable_sort_and_agg=True)
        field_b = FieldSchema('title', FieldType.TEXT, index=True, analyzer=AnalyzerType.MAXWORD)

        # search index 1: text type
        fields = [field_a, field_b]
        index_name = 'search_index_1'
        index_meta = SearchIndexMeta(fields, index_setting=None, index_sort=None)
        self.client_test.create_search_index(table_name, index_name, index_meta)

        search_indexes = self.client_test.list_search_index(table_name)
        self.assert_equal(1, len(search_indexes))
        self.assert_equal((table_name, index_name), search_indexes[0])

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name)
        self._check_index_meta(index_meta, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

    def test_create_search_index(self):
        table_name = 'full_text_search_test_' + self.get_python_version()
        table_meta = TableMeta(table_name, [('url', 'STRING')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        # search index: with highlight
        field_a = FieldSchema('type', FieldType.KEYWORD, index=True, enable_sort_and_agg=True)
        field_b = FieldSchema('title', FieldType.TEXT, index=True,
                    analyzer=AnalyzerType.MAXWORD, enable_highlighting=True)

        fields = [field_a, field_b]
        index_meta = SearchIndexMeta(fields, index_setting=None)
        index_name = 'search_index_2'
        self.client_test.create_search_index(table_name, index_name, index_meta)

        search_indexes = self.client_test.list_search_index(table_name)
        self.assert_equal(1, len(search_indexes))
        self.assert_equal((table_name, index_name), search_indexes[0])

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name)
        self._check_index_meta(index_meta, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

    def _test_match_all_query(self, table_name, index_name):
        # simple queries: match all query and scan to get all data with next token
        query = MatchAllQuery()

        search_response = self.client_test.search(table_name, index_name,
            SearchQuery(query, limit=10, get_total_count=True),
            columns_to_get=ColumnsToGet(['url', 'type', 'title'], ColumnReturnType.SPECIFIED))

        self.assert_equal(len(search_response.rows), 4)
        self.assert_equal(search_response.total_count, 4)
        self.assertTrue(search_response.is_all_succeed)

    def _test_match_query(self, table_name, index_name):
        # match web_1
        query = MatchQuery('title', '菊花茶')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 4, False, 4)

        # match nothing
        query = MatchQuery('title', '梨花茶')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 3, False, 3)

        # match web_1 and web_2
        query = MatchQuery('title', '花茶')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 3, False, 3)

        # match web_1 and web_3
        query = MatchQuery('title', '菊花')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 3, False, 3)

        # match web_2
        query = MatchQuery('title', '饮用花茶', operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        # match web_1 and web_2
        query = MatchQuery('title', '饮用花茶', operator=QueryOperator.OR)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 3, False, 3)

        # match web_2
        query = MatchQuery('title', '饮用花茶', operator=QueryOperator.OR, minimum_should_match=2)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

    def _test_match_phrase_query(self, table_name, index_name):
        query = MatchPhraseQuery('title', '野生菊花')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchPhraseQuery('title', '秋天菊花')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

    def _test_function_score_query(self, table_name, index_name):
        query = FunctionScoreQuery(
            MatchQuery('title', '花茶'),
            FieldValueFactor('pv')
        )

        sort = Sort(sorters=[ScoreSort(sort_order=SortOrder.DESC)])

        search_response = self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=10, get_total_count=True, sort=sort),
            columns_to_get = ColumnsToGet(['title', 'url'], ColumnReturnType.SPECIFIED)
        )

        self.assert_equal(3, len(search_response.rows))
        self.assert_equal(True, search_response.is_all_succeed)
        self.assert_equal(False, len(search_response.next_token) > 0)
        self.assert_equal(3, search_response.total_count)

        self.assert_equal('url', search_response.rows[0][0][0][0])
        self.assert_equal('http://a.com/019.html', search_response.rows[0][0][0][1])
        self.assert_equal('title', search_response.rows[0][1][0][0])
        self.assert_equal('菊花茶是一款大部分人都适合的经典花茶', search_response.rows[0][1][0][1])

        self.assert_equal('url', search_response.rows[1][0][0][0])
        self.assert_equal('http://b.com/news/001.html', search_response.rows[1][0][0][1])
        self.assert_equal('title', search_response.rows[1][1][0][0])
        self.assert_equal('秋天最适合饮用的几种花茶', search_response.rows[1][1][0][1])

        self.assert_equal('url', search_response.rows[2][0][0][0])
        self.assert_equal('http://a.com/001.html', search_response.rows[2][0][0][1])
        self.assert_equal('title', search_response.rows[2][1][0][0])
        self.assert_equal('求推荐几款菊花茶店铺', search_response.rows[2][1][0][1])

        self.assert_equal(3, len(search_response.search_hits))
        self.assert_equal(53, int(search_response.search_hits[0].score))
        self.assert_equal(11, int(search_response.search_hits[1].score))
        self.assert_equal(9, int(search_response.search_hits[2].score))

        self.assert_equal(0, len(search_response.search_hits[0].highlight_result.highlight_fields))
        self.assert_equal(0, len(search_response.search_hits[1].highlight_result.highlight_fields))
        self.assert_equal(0, len(search_response.search_hits[2].highlight_result.highlight_fields))

    def _test_highlight_query(self, table_name, index_name):
        query = FunctionScoreQuery(
            MatchQuery('title', '花茶'),
            FieldValueFactor('pv')
        )

        sort = Sort(sorters=[ScoreSort(sort_order=SortOrder.DESC)])

        highlight_parameter = HighlightParameter("title", 1, 18, '<b>', '</b>', HighlightFragmentOrder.TEXT_SEQUENCE)
        highlight_clause = Highlight([highlight_parameter], HighlightEncoder.PLAIN_MODE)

        search_query = SearchQuery(query, limit=10, get_total_count=True, sort=sort, highlight=highlight_clause)
        search_response = self.client_test.search(table_name, index_name,
            search_query,
            columns_to_get = ColumnsToGet(['title', 'url'], ColumnReturnType.SPECIFIED)
        )

        self.assert_equal(3, len(search_response.rows))
        self.assert_equal(True, search_response.is_all_succeed)
        self.assert_equal(False, len(search_response.next_token) > 0)
        self.assert_equal(3, search_response.total_count)

        self.assert_equal('url', search_response.rows[0][0][0][0])
        self.assert_equal('http://a.com/019.html', search_response.rows[0][0][0][1])
        self.assert_equal('title', search_response.rows[0][1][0][0])
        self.assert_equal('菊花茶是一款大部分人都适合的经典花茶', search_response.rows[0][1][0][1])

        self.assert_equal('url', search_response.rows[1][0][0][0])
        self.assert_equal('http://b.com/news/001.html', search_response.rows[1][0][0][1])
        self.assert_equal('title', search_response.rows[1][1][0][0])
        self.assert_equal('秋天最适合饮用的几种花茶', search_response.rows[1][1][0][1])

        self.assert_equal('url', search_response.rows[2][0][0][0])
        self.assert_equal('http://a.com/001.html', search_response.rows[2][0][0][1])
        self.assert_equal('title', search_response.rows[2][1][0][0])
        self.assert_equal('求推荐几款菊花茶店铺', search_response.rows[2][1][0][1])

        self.assert_equal(3, len(search_response.search_hits))
        self.assert_equal(53, int(search_response.search_hits[0].score))
        self.assert_equal(11, int(search_response.search_hits[1].score))
        self.assert_equal(9, int(search_response.search_hits[2].score))

        highlight_result_0 = search_response.search_hits[0].highlight_result
        self.assertTrue(highlight_result_0 != None)
        self.assert_equal(1, len(highlight_result_0.highlight_fields))
        self.assert_equal('title', highlight_result_0.highlight_fields[0].field_name)
        self.assert_equal(1, len(highlight_result_0.highlight_fields[0].field_fragments))
        self.assert_equal('菊<b>花茶</b>是一款大部分人都适合的经典<b>花茶</b>', highlight_result_0.highlight_fields[0].field_fragments[0])

        highlight_result_1 = search_response.search_hits[1].highlight_result
        self.assertTrue(highlight_result_1 != None)
        self.assert_equal(1, len(highlight_result_1.highlight_fields))
        self.assert_equal('title', highlight_result_1.highlight_fields[0].field_name)
        self.assert_equal(1, len(highlight_result_1.highlight_fields[0].field_fragments))
        self.assert_equal('秋天最适合饮用的几种<b>花茶</b>', highlight_result_1.highlight_fields[0].field_fragments[0])

        highlight_result_2 = search_response.search_hits[2].highlight_result
        self.assertTrue(highlight_result_2 != None)
        self.assert_equal(1, len(highlight_result_2.highlight_fields))
        self.assert_equal('title', highlight_result_2.highlight_fields[0].field_name)
        self.assert_equal(1, len(highlight_result_2.highlight_fields[0].field_fragments))
        self.assert_equal('求推荐几款菊<b>花茶</b>店铺', highlight_result_2.highlight_fields[0].field_fragments[0])

    def _test_highlight_query_with_failed(self, table_name, index_name):
        query = FunctionScoreQuery(
            MatchQuery('author', 'wang'),
            FieldValueFactor('pv')
        )

        highlight_parameter = HighlightParameter("author", 1, 18, '<b>', '</b>', HighlightFragmentOrder.TEXT_SEQUENCE)
        highlight_clause = Highlight([highlight_parameter], HighlightEncoder.PLAIN_MODE)

        search_query = SearchQuery(query, limit=10, get_total_count=True, highlight = highlight_clause)

        try:
            search_response = self.client_test.search(table_name, index_name,
                search_query,
                ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)
            )
        except OTSServiceError as e:
            self.assert_equal('OTSParameterInvalid', e.get_error_code())
            self.assert_equal('highlighting is not supported for field[author]', e.get_error_message())

    def _check_query_result(self, search_response, rows_count, has_next_token, expect_total_count):
        self.assert_equal(len(search_response.rows), rows_count)
        self.assert_equal(search_response.is_all_succeed, True)
        self.assert_equal(len(search_response.next_token) > 0, has_next_token)
        self.assert_equal(search_response.total_count, expect_total_count)

        self.assert_equal(len(search_response.v1_response()[0]), rows_count)
        self.assert_equal(search_response.v1_response()[3], True)
        self.assert_equal(len(search_response.v1_response()[1]) > 0, has_next_token)
        self.assert_equal(search_response.v1_response()[2], expect_total_count)

        pos = 0
        for item in search_response:
            if pos == 0:
                self.assert_equal(len(item), rows_count)
            elif pos == 1:
                self.assert_equal(len(item) > 0, has_next_token)
            elif pos == 2:
                self.assert_equal(item, expect_total_count)
            elif pos == 3:
                self.assert_equal(item, True)
            pos += 1

        return search_response.rows

    def test_queries(self):
        table_name = 'full_text_search_test_' + self.get_python_version()
        index_name = 'search_index'

        self._prepare_table(table_name)
        self._prepare_data(table_name)
        self._prepare_index(table_name, index_name)

        self._test_match_all_query(table_name, index_name)
        self._test_match_query(table_name, index_name)
        self._test_match_phrase_query(table_name, index_name)
        self._test_function_score_query(table_name, index_name)
        self._test_highlight_query(table_name, index_name)
        self._test_highlight_query_with_failed(table_name, index_name)

    def _prepare_data(self, table_name):
        '''菊花茶会分词成：菊花、花茶'''

        web_1_url = 'http://a.com/001.html'
        web_1_type = 'forum'
        web_1_title = '求推荐几款菊花茶店铺'
        web_1_pv = 25

        web_2_url = 'http://b.com/news/001.html'
        web_2_type = 'news'
        web_2_title = '秋天最适合饮用的几种花茶'
        web_2_pv = 30

        web_3_url = 'http://a.com/011.html'
        web_3_type = 'forum'
        web_3_title = '苕溪边上有一大片野生菊花'
        web_3_pv = 100

        web_4_url = 'http://a.com/019.html'
        web_4_type = 'forum'
        web_4_title = '菊花茶是一款大部分人都适合的经典花茶'
        web_4_pv = 120

        pk = [('url', web_1_url)]
        cols = [('type', web_1_type), ('title', web_1_title), ('pv', web_1_pv)]
        self.client_test.put_row(table_name, Row(pk, cols))

        pk = [('url', web_2_url)]
        cols = [('type', web_2_type), ('title', web_2_title), ('pv', web_2_pv)]
        self.client_test.put_row(table_name, Row(pk, cols))

        pk = [('url', web_3_url)]
        cols = [('type', web_3_type), ('title', web_3_title), ('pv', web_3_pv)]
        self.client_test.put_row(table_name, Row(pk, cols))

        pk = [('url', web_4_url)]
        cols = [('type', web_4_type), ('title', web_4_title), ('pv', web_4_pv)]
        self.client_test.put_row(table_name, Row(pk, cols))

    def _prepare_table(self, table_name):
        table_meta = TableMeta(table_name, [('url', 'STRING')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

    def _prepare_index(self, table_name, index_name, with_nested=False):
        field_a = FieldSchema('url', FieldType.KEYWORD, index=True, enable_sort_and_agg=True)
        field_b = FieldSchema('title', FieldType.TEXT, index=True, analyzer=AnalyzerType.MAXWORD, enable_highlighting = True)
        field_c = FieldSchema('type', FieldType.KEYWORD, index=True, enable_sort_and_agg=True)
        field_d = FieldSchema('pv', FieldType.LONG, index=True, enable_sort_and_agg=True)
        field_e = FieldSchema('author', FieldType.TEXT, index=True, enable_sort_and_agg=False)

        fields = [field_a, field_b, field_c, field_d, field_e]
        index_setting = None
        index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)])
        index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort) # default with index sort
        self.client_test.create_search_index(table_name, index_name, index_meta)
        self.wait_for_search_index_ready(self.client_test, table_name, index_name)

if __name__ == '__main__':
    unittest.main()
