# -*- coding: utf8 -*-

import unittest
from lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging
import json

class SearchIndexTest(APITestBase):

    def _check_field_schema(self, expect_field_schema, actual_field_schema):
        self.assert_equal(expect_field_schema.field_name, actual_field_schema.field_name)
        self.assert_equal(expect_field_schema.field_type, actual_field_schema.field_type)
        if actual_field_schema.field_type != FieldType.NESTED:
            self.assert_equal(expect_field_schema.index if expect_field_schema.index else True, actual_field_schema.index)
            self.assert_equal(expect_field_schema.store if expect_field_schema.store else False, actual_field_schema.store)
            self.assert_equal(expect_field_schema.is_array if expect_field_schema.is_array else False, actual_field_schema.is_array)
            self.assert_equal(expect_field_schema.enable_sort_and_agg if expect_field_schema.enable_sort_and_agg else False, actual_field_schema.enable_sort_and_agg)

        if actual_field_schema.field_type == FieldType.TEXT:
            self.assert_equal(expect_field_schema.analyzer if expect_field_schema.analyzer is not None else AnalyzerType.SINGLEWORD, actual_field_schema.analyzer)

        if actual_field_schema.sub_field_schemas:
            for i in range(len(actual_field_schema.sub_field_schemas)):
                self._check_field_schema(expect_field_schema.sub_field_schemas[i], actual_field_schema.sub_field_schemas[i])

    def _check_sorter(self, expect_sorter, actual_sorter):
        if isinstance(expect_sorter, FieldSort):
            self.assertTrue(isinstance(actual_sorter, FieldSort))
        elif isinstance(expect_sorter, PrimaryKeySort):
            self.assertTrue(isinstance(actual_sorter, PrimaryKeySort))
        else:
            self.assertTrue(False)

    def _check_index_meta(self, expect_index_meta, actual_index_meta, with_nested=False):
        self.assert_equal(len(expect_index_meta.fields), len(actual_index_meta.fields))
        for i in range(len(actual_index_meta.fields)):
            self._check_field_schema(expect_index_meta.fields[i], actual_index_meta.fields[i])

        # check index setting
        if expect_index_meta.index_setting is None:
            self.assert_equal(actual_index_meta.index_setting.routing_fields, [])
        else:
            self.assert_equal(actual_index_meta.index_setting.routing_fields, expect_index_meta.index_setting.routing_fields)

        # check index sort
        if expect_index_meta.index_sort is None:
            if with_nested:
                self.assert_equal(len(actual_index_meta.index_sort.sorters), 0)
            else:
                self.assert_equal(len(actual_index_meta.index_sort.sorters), 1)
                self._check_sorter(actual_index_meta.index_sort.sorters[0], PrimaryKeySort(SortOrder.ASC))
        else:
            for i in range(len(expect_index_meta.index_sort.sorters)):
                self._check_sorter(expect_index_meta.index_sort.sorters[i], actual_index_meta.index_sort.sorters[i])

    def test_create_search_index(self):
        table_name = 'SearchIndexTest_' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK1', 'STRING'), ('PK2', 'INTEGER')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
        field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
        field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
        nested_field = FieldSchema('n', FieldType.NESTED, sub_field_schemas=
            [
                FieldSchema('nk', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
                FieldSchema('nt', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD),
                FieldSchema('ng', FieldType.GEOPOINT, index=True, store=True, enable_sort_and_agg=True)
                ])
        # search index 1: simple schema
        fields = [field_a, field_b, field_c, field_d]
        index_name_1 = 'search_index_1'
        index_meta_1 = SearchIndexMeta(fields, index_setting=None, index_sort=None)
        self.client_test.create_search_index(table_name, index_name_1, index_meta_1)

        # search index 2: with sort
        index_sort = Sort([PrimaryKeySort(sort_order=SortOrder.DESC)])
        index_meta_2 = SearchIndexMeta(fields, index_setting=None, index_sort=index_sort)
        index_name_2 = 'search_index_2'
        self.client_test.create_search_index(table_name, index_name_2, index_meta_2)

        # search index 3: with nested
        fields = [field_a, field_b, field_c, field_d, nested_field]
        index_name_3 = 'search_index_3'
        index_meta_3 = SearchIndexMeta(fields, index_setting=None, index_sort=None)
        self.client_test.create_search_index(table_name, index_name_3, index_meta_3)

        # search index 4: with routing keys
        index_setting = IndexSetting(routing_fields=['PK2'])
        fields = [field_a, field_b, field_c, field_d]
        index_name_4 = 'search_index_4'
        index_meta_4 = SearchIndexMeta(fields, index_setting=index_setting, index_sort=None)
        self.client_test.create_search_index(table_name, index_name_4, index_meta_4)

        search_indexes = self.client_test.list_search_index(table_name)
        self.assert_equal(4, len(search_indexes))
        search_indexes.sort()
        self.assert_equal((table_name, index_name_1), search_indexes[0])
        self.assert_equal((table_name, index_name_2), search_indexes[1])
        self.assert_equal((table_name, index_name_3), search_indexes[2])
        self.assert_equal((table_name, index_name_4), search_indexes[3])

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_1)
        self._check_index_meta(index_meta_1, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_2)
        self._check_index_meta(index_meta_2, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_3)
        self._check_index_meta(index_meta_3, index_meta, with_nested=True)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

        index_meta, sync_stat = self.client_test.describe_search_index(table_name, index_name_4)
        self._check_index_meta(index_meta_4, index_meta)
        self.assertTrue(sync_stat.sync_phase == SyncPhase.INCR or sync_stat.sync_phase == SyncPhase.FULL)
        self.assertTrue(sync_stat.current_sync_timestamp >= 0)

    def _test_match_all_query(self, table_name, index_name):
        # simple queries: match all query and scan to get all data with next token
        query = MatchAllQuery()
        all_rows = []
        next_token = None

        while not all_rows or next_token:
            search_response = self.client_test.search(table_name, index_name,
                SearchQuery(query, next_token=next_token, limit=100, get_total_count=True),
                columns_to_get=ColumnsToGet(['k', 't', 'g', 'ka', 'la'], ColumnReturnType.SPECIFIED))
            if search_response.next_token:
                self.assert_equal(len(search_response.rows), 100)
            self.assert_equal(search_response.total_count, 1000)
            self.assertTrue(search_response.is_all_succeed)
            all_rows.extend(search_response.rows)

        self.assert_equal(len(all_rows), 100)
        for i in range(len(all_rows)):
            row = all_rows[i]
            pk = row[0]
            cols = row[1]
            self.assert_equal('pk_' + str(i % 10), pk[1][1])
            self.assert_equal(i, pk[0][1])
            self.assert_equal(len(cols), 5)
            self.assert_equal(cols[1][:2], ('k', 'key%03d' % i))
            self.assert_equal(cols[2][:2], ('ka', '["a", "b", "%d"]' % i))
            self.assert_equal(cols[3][:2], ('la', '[-1, %d]' % i))
            self.assert_equal(cols[4][:2], ('t', 'this is %d' % i))

    def _test_match_query(self, table_name, index_name):
        query = MatchQuery('t', 'this')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchQuery('t', 'not')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

        query = MatchQuery('t', 'this is 0')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchQuery('t', 'this is 0', minimum_should_match=3)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'this is 0', minimum_should_match=3, operator=QueryOperator.OR)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'this is 0', operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchQuery('t', 'is this 0', minimum_should_match=0, operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        # zero should queries
        query = MatchQuery('t', 'this is 0', minimum_should_match=1, operator=QueryOperator.AND)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

    def _test_match_phrase_query(self, table_name, index_name):
        query = MatchPhraseQuery('t', 'this is')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = MatchPhraseQuery('t', 'this is 1')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = MatchPhraseQuery('k', 'key999')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

    def _test_term_query(self, table_name, index_name):
        query = TermQuery('k', 'key000')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('t', '100')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('l', '900')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('la', '900')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        query = TermQuery('ka', 'a')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = TermQuery('b', True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 500)

        query = TermQuery('d', 0.1)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

    def _test_range_query(self, table_name, index_name):
        query = RangeQuery('k', 'key100', 'key200', include_lower=False, include_upper=False)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 99, False, 99)

        query = RangeQuery('k', 'key100', 'key200', include_lower=True, include_upper=True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 101)

        query = RangeQuery('la', '100', '300', include_lower=True, include_upper=True)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 201)

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

    def _test_prefix_query(self, table_name, index_name):
        query = PrefixQuery('k', 'key')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = PrefixQuery('k', 'key00')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 10, False, 10)

        query = PrefixQuery('t', 'this')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = PrefixQuery('t', 'this is')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

    def _test_wildcard_query(self, table_name, index_name):
        query = WildcardQuery('t', 't*')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = WildcardQuery('k', 'key00*')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 10, False, 10)

    def _test_terms_query(self, table_name, index_name):
        query = TermsQuery('k', ['key000', 'key100', 'key888', 'key999', 'key908', 'key1000'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 5, False, 5)

        query = TermsQuery('t', ['this', 'is'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        query = TermsQuery('l', [0, 999, 1000, 1002])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 2, False, 2)

    def _test_bool_query(self, table_name, index_name):
        # k > 'key100' and not (150 <= l < 200) and l < 250
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', include_lower=False)
            ],
            must_not_queries=[
                RangeQuery('l', 150, 200, include_lower=True, include_upper=False)
            ],
            filter_queries=[
                RangeQuery('l', range_to=250, include_upper=False)
            ],
            should_queries=[]
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.DESC)]), limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 99, False, 99)

        values = [row[1][5][1] for row in rows]
        expect_values = []
        expect_values.extend(range(101, 150))
        expect_values.extend(range(200, 250))
        expect_values.reverse()

        self.assert_equal(values, expect_values)

        # k > 'key100' and (l > 110 and l < 200) and not (k = 'key121')
        # and should_queries(k > 'key120' or l < 300, minimum_should_match=2)
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', include_lower=False),
                BoolQuery(
                    must_queries=[
                        RangeQuery('l', range_from=110, include_lower=False),
                        RangeQuery('l', range_to=200, include_upper=False)
                    ],
                )
            ],
            must_not_queries=[
                TermQuery('k', 'key121')
            ],
            should_queries=[
                RangeQuery('k', range_from='key120', include_lower=False),
                RangeQuery('l', range_to=300, include_upper=130)
            ],
            minimum_should_match=2
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.ASC)]), limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 78, False, 78)

        values = [row[1][5][1] for row in rows]
        expect_values = []
        expect_values.extend(range(122, 200))
        self.assert_equal(values, expect_values)

    def _test_geo_distance_query(self, table_name, index_name):
        query = GeoDistanceQuery('g', '32.5,116.5', 300000)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 668)

    def _test_geo_bounding_box_query(self, table_name, index_name):
        query = GeoBoundingBoxQuery('g', '30.9,112.0', '30.2,119.0')
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 500)

    def _test_geo_polygon_query(self, table_name, index_name):
        query = GeoPolygonQuery('g', ['30.9,112.0', '30.5,115.0', '30.3, 117.0', '30.2,119.0'])
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 90, False, 90)

    def _test_nested_query(self, table_name, index_name):
        nested_query = TermQuery('n.nk', 'key199')
        query = NestedQuery('n', nested_query)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 1, False, 1)

        nested_query = RangeQuery('n.nl', range_from=100, range_to=300, include_lower=True, include_upper=True)
        query = NestedQuery('n', nested_query)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, False, 201)

    def _test_function_score_query(self, table_name, index_name):
        query = FunctionScoreQuery(
            RangeQuery('l', range_from=100, range_to=300),
            FieldValueFactor('l')
        )

        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 200)

    def _test_sort(self, table_name, index_name):
        query = MatchQuery('t', 'this is')
        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(
                query, limit=100, get_total_count=True,
                sort=Sort(
                    sorters = [FieldSort('l', SortOrder.ASC)]
                )
                ),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        values = [row[1][5][1] for row in rows]
        expect_values = []
        expect_values.extend(range(0, 100))
        self.assert_equal(values, expect_values)

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name,
            SearchQuery(
                query, limit=100, get_total_count=True,
                sort=Sort(
                    sorters = [FieldSort('l', SortOrder.DESC)]
                )
                ),
            ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 100, True, 1000)

        values = [row[1][5][1] for row in rows]
        expect_values = []
        expect_values.extend(range(900, 1000))
        expect_values.reverse()
        self.assert_equal(values, expect_values)

    def _test_search_with_routing_keys(self, table_name, index_name):
        query = RangeQuery('l', range_from=100, range_to=300)
        self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(query, limit=100, get_total_count=True),
            ColumnsToGet(return_type=ColumnReturnType.ALL), routing_keys=[[('PK1', 0)]]
        ), 100, True, 200)

    def _test_exists_query(self, table_name, index_name):
        # 'key100' < k <= 'key200' and b is not null and not (150 <= l < 200)
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', range_to='key200', include_lower=False, include_upper=True),
                ExistsQuery('b')
            ],
            must_not_queries=[
                RangeQuery('l', 150, 200, include_lower=True, include_upper=False)
            ]
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.DESC)]), limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 50, False, 50)

        # 'key100' < k <= 'key200' and b is null and not (150 <= l < 200)
        bool_query = BoolQuery(
            must_queries=[
                RangeQuery('k', range_from='key100', range_to='key200', include_lower=False, include_upper=True)
            ],
            must_not_queries=[
                RangeQuery('l', 150, 200, include_lower=True, include_upper=False),
                ExistsQuery('b')
            ]
        )

        rows = self._check_query_result(self.client_test.search(
            table_name, index_name, SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.DESC)]), limit=100, get_total_count=True), ColumnsToGet(return_type=ColumnReturnType.ALL)
        ), 0, False, 0)

    def test_queries(self):
        table_name = 'SearchIndexQueryTest_' + self.get_python_version()
        index_name = 'search_index'
        nested_index_name = 'search_index_nested'

        self._prepare_table(table_name)
        self._prepare_index(table_name, index_name, with_nested=False)
        self._prepare_index(table_name, nested_index_name, with_nested=True)
        self._prepare_data(table_name, 1000)
        time.sleep(100)
        self._test_match_all_query(table_name, index_name)
        self._test_match_query(table_name, index_name)
        self._test_match_phrase_query(table_name, index_name)
        self._test_term_query(table_name, index_name)
        self._test_range_query(table_name, index_name)
        self._test_prefix_query(table_name, index_name)
        self._test_wildcard_query(table_name, index_name)
        self._test_terms_query(table_name, index_name)
        self._test_bool_query(table_name, index_name)
        self._test_geo_distance_query(table_name, index_name)
        self._test_geo_bounding_box_query(table_name, index_name)
        self._test_geo_polygon_query(table_name, index_name)
        self._test_nested_query(table_name, nested_index_name)
        self._test_function_score_query(table_name, index_name)
        self._test_exists_query(table_name, index_name)
        self._test_sort(table_name, index_name)
        self._test_search_with_routing_keys(table_name, index_name)

    def _prepare_data(self, table_name, rows_count):
        for i in range(rows_count):
            pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
            lj = int(i / 100)
            li = i % 100
            cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)),
                ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "b", "%d"]' % i),
                ('la', '[-1, %d]' % i), ('l', i),
                ('b', i % 2 == 0), ('d', 0.1),
                ('n', json.dumps([{'nk':'key%03d' % i, 'nl':i, 'nt':'this is in nested ' + str(i)}]))]

            self.client_test.put_row(table_name, Row(pk, cols))

    def _prepare_table(self, table_name):
        table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

    def _prepare_index(self, table_name, index_name, with_nested=False):
        field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
        field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
        field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
        field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
        field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
        field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
        field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)
        if with_nested:
            field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
                FieldSchema('nk', FieldType.KEYWORD, index=True, store=True),
                FieldSchema('nl', FieldType.LONG, index=True, store=True),
                FieldSchema('nt', FieldType.TEXT, index=True, store=True),
            ])

        fields = [field_a, field_b, field_c, field_d, field_e, field_f, field_g, field_h]
        if with_nested:
            fields.append(field_n)
        index_setting = IndexSetting(routing_fields=['PK1'])
        index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)]) if not with_nested else None
        index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort) # default with index sort
        self.client_test.create_search_index(table_name, index_name, index_meta)

if __name__ == '__main__':
    unittest.main()
