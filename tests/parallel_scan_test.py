# -*- coding: utf8 -*-

import unittest
from lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging
import json

class ParallelScanTest(APITestBase):
    def setUp(self):
        APITestBase.setUp(self)

        self.table_name = 'SearchIndexParallelScanTest_' + self.get_python_version()
        self.index_name = 'search_index'
        self.rows_count = 100

        self._prepare_table(self.table_name)
        self._prepare_index(self.table_name, self.index_name)
        self._prepare_data(self.table_name, self.rows_count)

        time.sleep(30) 

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

    def _prepare_index(self, table_name, index_name):
        field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
        field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
        field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
        field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
        field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
        field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
        field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
        field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)
        fields = [field_a, field_b, field_c, field_d, field_e, field_f, field_g, field_h]

        index_setting = IndexSetting(routing_fields=['PK1'])
        index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)]) 
        index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort) # default with index sort
        self.client_test.create_search_index(table_name, index_name, index_meta)

    def test_compute_splits_normal(self):
        compute_splits_response = self.client_test.compute_splits(self.table_name, self.index_name)

        self.assertTrue(len(compute_splits_response.session_id) > 0)
        self.assert_equal(1, compute_splits_response.splits_size)

    def test_compute_splits_with_tablename_is_none(self):
        try:
            self.client_test.compute_splits(None, self.index_name)
        except OTSClientError as e:
            self.assert_equal("table_name must not be None", e.get_error_message())

    def test_compute_splits_with_indexname_is_none(self):
        try:
            self.client_test.compute_splits(self.table_name, None)
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "[search index splits options] must not be null")

    def test_compute_splits_with_indexname_is_not_eixt(self):
        try:
            self.client_test.compute_splits(self.table_name, 'not_exist')
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSMetaNotMatch", "search index [not_exist] does not exist")

    def test_compute_splits_with_tablename_is_not_exist(self):
        try:
            self.client_test.compute_splits('not_exist', self.index_name)
        except OTSServiceError as e:            
            self.assert_error(e, 400, "OTSParameterInvalid", "table [not_exist] does not exist")

    def test_parallel_scan_normal(self):
        compute_splits_response = self.client_test.compute_splits(self.table_name, self.index_name)

        self.assert_equal(1, compute_splits_response.splits_size)
        self.assertTrue(len(compute_splits_response.session_id) > 0)

        self.assert_equal(compute_splits_response.session_id, compute_splits_response.v1_response()[0])
        self.assert_equal(compute_splits_response.splits_size, compute_splits_response.v1_response()[1])

        pos = 0
        for item in compute_splits_response:
            if pos == 0:
                self.assert_equal(compute_splits_response.session_id, item)
            elif pos == 1:
                self.assert_equal(compute_splits_response.splits_size, item)
            pos += 1

        query = TermQuery('d', 0.1)
        scan_query = ScanQuery(query, limit = 70, next_token = None, current_parallel_id = 0, 
                               max_parallel = compute_splits_response.splits_size, alive_time = 30)
        parallel_scan_response = self.client_test.parallel_scan(
            self.table_name, self.index_name, scan_query, compute_splits_response.session_id, 
            columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))
        
        self.assert_equal(70, len(parallel_scan_response.rows))
        self.assertTrue(parallel_scan_response.next_token is not None)

        self.assert_equal(70, len(parallel_scan_response.v1_response()[0]))
        self.assertTrue(parallel_scan_response.v1_response()[1] is not None)

        pos = 0
        for item in parallel_scan_response:
            if pos == 0:
                self.assert_equal(70, len(item))
            elif pos == 1:
                self.assertTrue(item is not None)
            pos += 1

        scan_query_2 = ScanQuery(query, limit = 70, next_token = parallel_scan_response.next_token, current_parallel_id = 0, 
                                 max_parallel = compute_splits_response.splits_size, alive_time = 30)
        parallel_scan_response2 = self.client_test.parallel_scan(
            self.table_name, self.index_name, scan_query_2, compute_splits_response.session_id, 
            columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))
        
        self.assert_equal(30, len(parallel_scan_response2.rows))
        self.assertTrue(parallel_scan_response2.next_token != '')

        scan_query_3 = ScanQuery(query, limit = 70, next_token = parallel_scan_response2.next_token, current_parallel_id = 0, 
                               max_parallel = compute_splits_response.splits_size, alive_time = 30)
        parallel_scan_response3 = self.client_test.parallel_scan(
            self.table_name, self.index_name, scan_query_3, compute_splits_response.session_id, 
            columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))
        
        self.assert_equal(0, len(parallel_scan_response3.rows))
        self.assertTrue(parallel_scan_response3.next_token == '')

        
    def test_parallel_scan_with_invalid_parallel_id(self):
        compute_splits_response = self.client_test.compute_splits(self.table_name, self.index_name)        

        query = TermQuery('d', 0.1)
        
        try:
            scan_query = ScanQuery(query, limit = 700, next_token = None, current_parallel_id = compute_splits_response.splits_size + 100,
                                   max_parallel = compute_splits_response.splits_size, alive_time = 30)

            parallel_scan_response = self.client_test.parallel_scan(
                self.table_name, self.index_name, scan_query, compute_splits_response.session_id, 
                columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))

            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "[parallel_scan.current_parallel_id] must in [0, max_parallel), current max_parallel is 1, current_parallel_id is 101")
        
        
    def test_parallel_scan_with_invalid_max_parallel(self):
        compute_splits_response = self.client_test.compute_splits(self.table_name, self.index_name)        

        query = TermQuery('d', 0.1)
        
        try:
            scan_query = ScanQuery(query, limit = 70, next_token = None, current_parallel_id = 0, 
                                   max_parallel = compute_splits_response.splits_size + 1, alive_time = 30)

            self.client_test.parallel_scan(
                self.table_name, self.index_name, scan_query, compute_splits_response.session_id, 
                columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))
            self.assertTrue(False)
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "[parallel_scan.max_parallel_id] must in (0 ,1], current max_parallel_id is 2")

    def test_parallel_scan_with_expired(self):
        alive_time_s = 30
        compute_splits_response = self.client_test.compute_splits(self.table_name, self.index_name)        

        query = TermQuery('d', 0.1)
        scan_query = ScanQuery(query, limit = 70, next_token = None, current_parallel_id = 0, 
                               max_parallel = compute_splits_response.splits_size, alive_time =alive_time_s)
        parallel_scan_response = self.client_test.parallel_scan(
            self.table_name, self.index_name, scan_query, compute_splits_response.session_id, 
            columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))
        
        self.assert_equal(70, len(parallel_scan_response.rows))
        self.assertTrue(parallel_scan_response.next_token is not None)

        time.sleep(alive_time_s + 30)

        try:
            scan_query_2 = ScanQuery(query, limit = 70, next_token = parallel_scan_response.next_token, current_parallel_id = 0, 
                                     max_parallel = compute_splits_response.splits_size, alive_time = 30)
            parallel_scan_response2 = self.client_test.parallel_scan(
                self.table_name, self.index_name, scan_query_2, compute_splits_response.session_id, 
                columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSSessionExpired", "ScanQuery'session is expired, please retry ComputeSplitsRequest and ScanQuery.")
    

if __name__ == '__main__':
    unittest.main()
