# -*- coding: utf8 -*-

import unittest
from lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging
import json

class AggTest(APITestBase):
    def setUp(self):
        APITestBase.setUp(self)

        self.table_name = 'AggTest_' + self.get_python_version()
        self.index_name = 'search_index'
        self.rows_count = 100

        self._prepare_table()
        self._prepare_index()
        self._prepare_data()

        time.sleep(30) 

    def _prepare_data(self):
        for i in range(self.rows_count):
            pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
            lj = int(i / 100)
            li = i % 100
            cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)),
                ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "b", "%d"]' % i),
                ('la', '[-1, %d]' % i), ('l', i),
                ('b', i % 2 == 0), ('d', 0.1),
                ('n', json.dumps([{'nk':'key%03d' % i, 'nl':i, 'nt':'this is in nested ' + str(i)}]))]

            self.client_test.put_row(self.table_name, Row(pk, cols))

    def _prepare_table(self):
        table_meta = TableMeta(self.table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(self.table_name)

    def _prepare_index(self):
        field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
        field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
        field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
        field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
        field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
        field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
        field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)
        fields = [field_a, field_b, field_c, field_d, field_e, field_f, field_g, field_h]

        field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
            FieldSchema('nk', FieldType.KEYWORD, index=True, store=True),
            FieldSchema('nl', FieldType.LONG, index=True, store=True),
            FieldSchema('nt', FieldType.TEXT, index=True, store=True),
        ])
        fields.append(field_n)

        index_setting = IndexSetting(routing_fields=['PK1'])
        index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=None)
        self.client_test.create_search_index(self.table_name, self.index_name, index_meta)

    def test_max_agg_normal(self):
        # long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Max('l')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(99, search_response.agg_results[0].value)

        # double type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Max('d')]),
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(0.1, search_response.agg_results[0].value)

        # nested long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Max('n.nl')]),
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(99, search_response.agg_results[0].value)

        # array long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Max('la')]),
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(99, search_response.agg_results[0].value)

    def test_max_agg_with_invalid_type(self):
        self._do_test_agg_with_invalid_type(Max('k'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Max('t'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Max('g'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Max('ka'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Max('b'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Max('n.nk'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Max('n.nt'), "OTSParameterInvalid")
        
    def test_min_agg_normal(self):
        # long type
        search_response = self.client_test.search(
            self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Min('l')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(0, search_response.agg_results[0].value)

        # double type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Min('d')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assertAlmostEqual(0.1, search_response.agg_results[0].value, delta = 0.0000001)

        # nested long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Min('n.nl')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(0, search_response.agg_results[0].value)

        # array long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Min('la')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(-1, search_response.agg_results[0].value)

    def test_min_agg_with_invalid_type(self):
        self._do_test_agg_with_invalid_type(Min('k'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Min('t'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Min('g'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Min('ka'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Min('b'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Min('n.nk'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Min('n.nt'), "OTSParameterInvalid")

    def test_sum_agg_normal(self):
        # long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Sum('l')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(4950, search_response.agg_results[0].value)
        
        # double type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Sum('d')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assertAlmostEqual(10, search_response.agg_results[0].value, delta = 0.0000001)
        
        # nested long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Sum('n.nl')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(4950, search_response.agg_results[0].value)

        # array long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Sum('la')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(4850, search_response.agg_results[0].value)
        
    def test_sum_agg_with_invalid_type(self):
        self._do_test_agg_with_invalid_type(Sum('k'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Sum('t'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Sum('g'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Sum('ka'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Sum('b'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Sum('n.nk'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Sum('n.nt'), "OTSParameterInvalid")

    def test_avg_agg_normal(self):
        # long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Avg('l')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(49.5, search_response.agg_results[0].value)
        
        # double type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Avg('d')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assertAlmostEqual(0.1, search_response.agg_results[0].value, delta = 0.0000001)
        
        # nested long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Avg('n.nl')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(49.5, search_response.agg_results[0].value)

        # array long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Avg('la')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(24.25, search_response.agg_results[0].value)
        
    def test_avg_agg_with_invalid_type(self):
        self._do_test_agg_with_invalid_type(Avg('k'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Avg('t'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Avg('g'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Avg('ka'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Avg('b'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Avg('n.nk'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Avg('n.nt'), "OTSParameterInvalid")

    def test_count_agg_normal(self):
        # long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('l')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)
        
        # double type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('d')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # keyword type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('k')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # bool type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('b')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # geopoint type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('g')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)
        
        # nested long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('n.nl')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # nested keyword type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('n.nk')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # array long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('la')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(200, search_response.agg_results[0].value)

        # array keyword type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Count('ka')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(300, search_response.agg_results[0].value)

    def test_count_agg_with_invalid_type(self):
        self._do_test_agg_with_invalid_type(Count('t'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(Count('n.nt'), "OTSParameterInvalid")

    def test_distinct_count_agg_normal(self):
        # long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('l')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)
        
        # double type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('d')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(1, search_response.agg_results[0].value)

        # keyword type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('k')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # bool type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('b')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(2, search_response.agg_results[0].value)

        # geopoint type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('g')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)
        
        # nested long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('n.nl')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # nested keyword type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('n.nk')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(100, search_response.agg_results[0].value)

        # array long type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('la')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(101, search_response.agg_results[0].value)

        # array keyword type
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [DistinctCount('ka')]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(102, search_response.agg_results[0].value)

    def test_distinct_count_agg_with_invalid_type(self):
        self._do_test_agg_with_invalid_type(DistinctCount('t'), "OTSParameterInvalid")
        self._do_test_agg_with_invalid_type(DistinctCount('n.nt'), "OTSParameterInvalid")
        

    def test_top_rows_agg_normal(self):    
        # test top rows agg
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, 
                        aggs = [TopRows(limit=2, sort = Sort([PrimaryKeySort()]))]),
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.agg_results))
        self.assert_equal(2, len(search_response.agg_results[0].value))
        self.assert_equal("([(u'PK1', 0), (u'PK2', u'pk_0')], [])", str(search_response.agg_results[0].value[0]))
        self.assert_equal("([(u'PK1', 1), (u'PK2', u'pk_1')], [])", str(search_response.agg_results[0].value[1]))

    def test_top_rows_with_exception(self):
        try:
            self.client_test.search(self.table_name, self.index_name, 
                                    SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, 
                                                aggs = [TopRows(limit=10000, sort = Sort([PrimaryKeySort()]))]), 
                                    ColumnsToGet(return_type=ColumnReturnType.NONE))
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_equal(400, e.get_http_status())
            self.assert_equal("OTSParameterInvalid", e.get_error_code())
        
    def test_multiple_agg(self):
        # test multiple agg
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [Sum('l', name = 'a1'),Avg('l', name = "a2")]),
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(2, len(search_response.agg_results))
        self.assert_equal('a1', search_response.agg_results[0].name)
        self.assert_equal(4950, search_response.agg_results[0].value)
        self.assert_equal('a2', search_response.agg_results[1].name)
        self.assert_equal(49.5, search_response.agg_results[1].value)


    def _do_test_agg_with_invalid_type(self, agg, error_code):
        try:
            self.client_test.search(self.table_name, self.index_name, 
                                    SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, aggs = [agg]), 
                                    ColumnsToGet(return_type=ColumnReturnType.NONE))
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_equal(400, e.get_http_status())
            self.assert_equal(error_code, e.get_error_code())
        


if __name__ == '__main__':
    unittest.main()
