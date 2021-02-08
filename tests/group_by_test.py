# -*- coding: utf8 -*-

import unittest
from lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging
import json

class GroupByTest(APITestBase):
    def setUp(self):
        APITestBase.setUp(self)

        self.table_name = 'GroupByTest_' + self.get_python_version()
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
                ('la', '[-1, %d]' % i), ('l', i%3),
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

    def test_group_by_field(self):
        # # group by l
        group_by = GroupByField('l')
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(group_by_results))
        self.assert_equal(3, len(group_by_results[0].items))
        self.assert_equal('0', group_by_results[0].items[0].key)
        self.assert_equal(34, group_by_results[0].items[0].row_count)
        self.assert_equal('1', group_by_results[0].items[1].key)
        self.assert_equal(33, group_by_results[0].items[1].row_count)
        self.assert_equal('2', group_by_results[0].items[2].key)
        self.assert_equal(33, group_by_results[0].items[2].row_count)

        # group by l
        group_by = GroupByField('l', size = 2, group_by_sort = [RowCountSort(sort_order=SortOrder.ASC)])
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(group_by_results))
        self.assert_equal(2, len(group_by_results[0].items))
        self.assert_equal('1', group_by_results[0].items[0].key)
        self.assert_equal(33, group_by_results[0].items[0].row_count)
        self.assert_equal('2', group_by_results[0].items[1].key)
        self.assert_equal(33, group_by_results[0].items[1].row_count)

        # group by l
        sort = RowCountSort(sort_order = SortOrder.ASC)
        sub_agg = [TopRows(limit=3,sort=Sort([PrimaryKeySort(sort_order=SortOrder.DESC)]), name = 't1')]

        group_by = GroupByField('l', size = 2, group_by_sort = [sort], sub_aggs = sub_agg)
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.group_by_results))
        self.assert_equal(2, len(search_response.group_by_results[0].items))
        self.assert_equal('1', search_response.group_by_results[0].items[0].key)
        self.assert_equal(33, search_response.group_by_results[0].items[0].row_count)
        self.assert_equal(1, len(search_response.group_by_results[0].items[0].sub_aggs))
        self.assert_equal('t1', search_response.group_by_results[0].items[0].sub_aggs[0].name)
        self.assert_equal(3, len(search_response.group_by_results[0].items[0].sub_aggs[0].value))
        self.assert_equal("([(u'PK1', 97), (u'PK2', u'pk_7')], [])", 
                          str(search_response.group_by_results[0].items[0].sub_aggs[0].value[0]))
        self.assert_equal("([(u'PK1', 94), (u'PK2', u'pk_4')], [])", 
                          str(search_response.group_by_results[0].items[0].sub_aggs[0].value[1]))
        self.assert_equal("([(u'PK1', 91), (u'PK2', u'pk_1')], [])", 
                          str(search_response.group_by_results[0].items[0].sub_aggs[0].value[2]))

        self.assert_equal('2', search_response.group_by_results[0].items[1].key)
        self.assert_equal(33, search_response.group_by_results[0].items[1].row_count)

    def test_group_by_range(self):
        # group by l
        group_by = GroupByRange('la', ranges = [(0,10),(11,25)])
        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.group_by_results))
        self.assert_equal(2, len(search_response.group_by_results[0].items))
        self.assert_equal(0, search_response.group_by_results[0].items[0].range_from)
        self.assert_equal(10, search_response.group_by_results[0].items[0].range_to)
        self.assert_equal(10, search_response.group_by_results[0].items[0].row_count)
        self.assert_equal(11, search_response.group_by_results[0].items[1].range_from)
        self.assert_equal(25, search_response.group_by_results[0].items[1].range_to)
        self.assert_equal(14, search_response.group_by_results[0].items[1].row_count)

    def test_group_by_filter(self):
        # group by l
        filter1 = TermQuery('l', 1)
        filter2 = TermQuery('ka', "a")
        filters = [filter1, filter2]
        group_by = GroupByFilter(filters)

        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.group_by_results))
        self.assert_equal(2, len(search_response.group_by_results[0].items))
        self.assert_equal(33, search_response.group_by_results[0].items[0].row_count)
        self.assert_equal(100, search_response.group_by_results[0].items[1].row_count)

    def test_group_by_geo_distance(self):
        # group by l
        query = TermQuery('d', 0.1)
        group_by = GroupByGeoDistance(field_name = 'g', origin=GeoPoint(31, 116), ranges = [(0, 300000), (300000,1000000)])

        search_response = self.client_test.search(self.table_name, self.index_name, 
            SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
            ColumnsToGet(return_type=ColumnReturnType.NONE))
        
        self.assertTrue(search_response.is_all_succeed)
        self.assert_equal(1, len(search_response.group_by_results))
        self.assert_equal(2, len(search_response.group_by_results[0].items))
        self.assert_equal(0, search_response.group_by_results[0].items[0].range_from)
        self.assert_equal(300000, search_response.group_by_results[0].items[0].range_to)
        self.assert_equal(99, search_response.group_by_results[0].items[0].row_count)
        self.assert_equal(300000, search_response.group_by_results[0].items[1].range_from)
        self.assert_equal(1000000, search_response.group_by_results[0].items[1].range_to)
        self.assert_equal(1, search_response.group_by_results[0].items[1].row_count)

    def test_group_by_exception(self):
        self._do_test_group_by_exception(GroupByField('l', group_by_sort=[444]), 
                                         "Invalid sort type:<type 'int'>")
        self._do_test_group_by_exception(GroupByRange('l', ranges=[('a',11),(34.5,True)]), 
                                         "range.begin and range.end must be integer or float")
        self._do_test_group_by_exception(GroupByRange('l', ranges=[(1,11,34),(34.5)]), 
                                         "GroupByRange:range must be tuple, and length must equal 2")
        self._do_test_group_by_exception(GroupByFilter(filters=[(0,11)]), 
                                         "GroupByFilter:filter must be Query")
        self._do_test_group_by_exception(GroupByGeoDistance('g', origin = 11, ranges=[(0,100)]), 
                                         "GroupByGeoDistance:origin must not be None and must be GeoPoint")
        self._do_test_group_by_exception(GroupByGeoDistance('g', origin = GeoPoint(31, 115), ranges=[1000]), 
                                         "GroupByGeoDistance:range must be tuple, and length must equal 2")


    def _do_test_group_by_exception(self, group_by, error_message):
        try:
            self.client_test.search(self.table_name, self.index_name, 
                                    SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
                                    ColumnsToGet(return_type=ColumnReturnType.NONE))
            self.assertTrue(False)
        except OTSClientError as e:
            self.assert_equal(error_message, e.get_error_message())


if __name__ == '__main__':
    unittest.main()
