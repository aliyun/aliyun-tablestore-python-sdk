#*-coding:utf-8-*-
import subprocess

from . import test_config
from . import restriction
from builtins import int
from unittest import TestCase
from tablestore import *
from tablestore.error import *
from tablestore.retry import *
import types
import math
import time
import traceback
#import commands
import sys
import six

import os
import inspect
import logging

class APITestBase(TestCase):

    def __init__(self, methodName=None):
        TestCase.__init__(self, methodName=methodName)
        self.start_time = 0

        self.logger = logging.getLogger('APITestBase')
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler('tablestore_sdk_test.log')
        fh.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    def setUp(self):
        self.client_test = OTSClient(
            test_config.OTS_ENDPOINT,
            test_config.OTS_ID,
            test_config.OTS_SECRET,
            test_config.OTS_INSTANCE,
            logger_name = 'APITestBase',
            retry_policy=DefaultRetryPolicy(),
        )

        time.sleep(1) # to avoid too frequent table operations
        #return # for test, skip delete
        for table_name in self.client_test.list_table():
            if table_name.find(self.get_python_version()) != -1:
                for table_name, index_name in self.client_test.list_search_index(table_name):
                    self.client_test.delete_search_index(table_name, index_name)

                self.client_test.delete_table(table_name)

    def tearDown(self):
        pass

    def case_post_check(self):
        pass

    def assert_error(self, error, http_status, error_code, error_message):
        self.assert_equal(error.http_status, http_status)
        self.assert_equal(error.code, error_code)
        self.assert_equal(error.message.encode('utf-8'), error_message)

    def assert_false(self):
        self.logger.warn("\nAssertion Failed\n" + "".join(traceback.format_stack()))
        raise AssertionError

    def assert_equal(self, res, expect_res):
        if isinstance(res, six.binary_type):
            res = res.decode('utf-8')
        if isinstance(expect_res, six.binary_type):
            expect_res = expect_res.decode('utf-8')

        if res != expect_res:
            #self.logger.warn("\nAssertion Failed\nactual: %s\nexpect: %s\n" % (res.decode('utf-8'), expect_res.decode('utf-8')) + "".join(traceback.format_stack()))
            self.assertEqual(res, expect_res)

    def try_exhaust_cu(self, func, count, read_cu, write_cu):
        i = 0
        while True:
            try:
                self._try_exhaust_cu(func, count, read_cu, write_cu)
                break
            except Exception as e:
                i += 1
                self.logger.info("_try_exhaust_cu failed: %s" % str(e))
                if i >= 10:
                    self.assert_false()

    def _try_exhaust_cu(self, func, count, read_cu, write_cu):
        start_time = time.time()
        read_cu_sum = 0
        write_cu_sum = 0
        max_elapsed_time = 1.0 / (read_cu if read_cu != 0 else write_cu) * count
        self.logger.info("StartTime: %s, Count: %s, ReadCU: %s, WriteCU: %s, MaxElapsedTime: %s" % (start_time, count, read_cu, write_cu, max_elapsed_time));

        while count != 0:
            try:
                rc, wc = func()
                read_cu_sum += rc
                write_cu_sum += wc
                count -= 1
                self.logger.info("ReadCU: %s, WriteCU: %s, ReadCUSum: %s, WriteCUSum: %s, Count: %s" % (rc, wc, read_cu_sum, write_cu_sum, count))
            except OTSServiceError as e:
                self.assert_error(e, 403, "OTSNotEnoughCapacityUnit", "Remaining capacity unit is not enough.")

        end_time = time.time()
        interval = end_time - start_time
        if interval >= max_elapsed_time * 1.2:
                raise Exception('Exceed max elapsed_time: %s, %s' % (interval, max_elapsed_time))
        avg_read_cu = read_cu_sum / interval
        avg_write_cu = write_cu_sum / interval
        self.logger.info("Interval: %s, AvgReadCU: %s, AvgWriteCU: %s, ReadCU: %s, WriteCU: %s" % (interval, avg_read_cu, avg_write_cu, read_cu, write_cu))

        if read_cu != 0:
            self.assertTrue(avg_read_cu >= read_cu * 0.8)
            self.assertTrue(avg_read_cu < read_cu * 1.2)

        if write_cu != 0:
            self.assertTrue(avg_write_cu >= write_cu * 0.8)
            self.assertTrue(avg_write_cu < write_cu * 1.2)

    def try_to_consuming(self, table_name, pk_dict_exist, pk_dict_not_exist,
                         capacity_unit):
        read = capacity_unit.read
        write = capacity_unit.write
        no_check_flag = 0
        if read > 1 or write > 1:
            read = read - 1
            write = write - 1
            no_check_flag = 1
        columns = []
        column_value_size = 4096
        all_pk_length = self.get_row_size(pk_dict_exist, [])
        #write
        for i in range(write):
            if i is not 0:
                columns.append(('X' * i, 'X' * (column_value_size - i)))
            else:
                columns.append(('col0', 'X' * (column_value_size - all_pk_length - 10)))
        if write is not 0:
            row = Row(pk_dict_exist, {'put':columns})
            consumed_update,return_row = self.client_test.update_row(table_name, row, Condition(RowExistenceExpectation.IGNORE))
            expect_consumed = CapacityUnit(0, self.sum_CU_from_row(pk_dict_exist, columns))
            self.assert_consumed(consumed_update, expect_consumed)
            self.assert_equal(write, self.sum_CU_from_row(pk_dict_exist, columns))
        #consume(0, 1)
        if 1 == no_check_flag:
            try:
                consumed_update,return_row = self.client_test.delete_row(table_name, Row(pk_dict_not_exist), Condition(RowExistenceExpectation.IGNORE))
            except OTSServiceError as e:
                self.assert_false()

        #read
        while read >= write and write != 0:
            read = read - write
            consumed_read, return_row, token  = self.client_test.get_row(table_name, pk_dict_exist, max_version = 1)
            expect_consumed = CapacityUnit(self.sum_CU_from_row(pk_dict_exist, columns), 0)
            self.assert_consumed(consumed_read, expect_consumed)
            self.assert_equal(return_row.primary_key, pk_dict_exist)
        for i in range(read + no_check_flag):
            consumed_read, return_row, token= self.client_test.get_row(table_name, pk_dict_not_exist, max_version = 1)
            self.assert_consumed(consumed_read, CapacityUnit(1, 0))
            self.assert_equal(return_row, None)

    def check_CU_by_consuming(self, table_name, pk_dict_exist, pk_dict_not_exist,
                              capacity_unit):
        begin_time = time.time()
        self.try_to_consuming(table_name, pk_dict_exist, pk_dict_not_exist, capacity_unit)

    def assert_consumed(self, consumed, expect_consumed):
        if consumed == None or expect_consumed == None:
            self.assert_equal(consumed, expect_consumed)
        else:
            self.assert_equal(consumed.read, expect_consumed.read)
            self.assert_equal(consumed.write, expect_consumed.write)

    def assert_columns(self, columns, expect_columns):
        if columns is None:
            columns = []
        if expect_columns is None:
            expect_columns = []

        self.assert_equal(len(columns), len(expect_columns))
        for index in range(len(columns)):
            self.assert_equal(columns[index][0], expect_columns[index][0])
            self.assert_equal(columns[index][1], expect_columns[index][1])


    def assert_RowDataItem_equal(self, response, expect_response):
        self.assert_equal(len(response), len(expect_response))
        for i in range(len(response)):
            self.assert_equal(len(response[i]), len(expect_response[i]))
            for j in range(len(response[i])):

                if expect_response[i][j].is_ok and not response[i][j].is_ok:
                    raise Exception("BatchGetRow failed on at least one row, ErrorCode: %s ErrorMessage: %s" % (response[i][j].error_code, response[i][j].error_message))

                self.assert_equal(response[i][j].is_ok, expect_response[i][j].is_ok)
                if expect_response[i][j].is_ok:
                    self.assert_consumed(response[i][j].consumed, expect_response[i][j].consumed)
                    self.assert_equal(response[i][j].primary_key_columns, expect_response[i][j].primary_key_columns)
                    self.assert_equal(response[i][j].attribute_columns, expect_response[i][j].attribute_columns)
                else:
                    self.assert_equal(response[i][j].error_code, expect_response[i][j].error_code)
                    self.assert_equal(response[i][j].error_message, expect_response[i][j].error_message)
                    self.assert_consumed(response[i][j].consumed, expect_response[i][j].consumed)

    def assert_BatchWriteRowResponseItem(self, response, expect_response):



        self.assert_equal(len(response), len(expect_response))
        item_list = ['put', 'update', 'delete']
        for i in range(len(response)):
            for item in item_list:
                if response[i].has_key(item) == expect_response[i].has_key(item):
                    if response[i].has_key(item):
                        self.assert_equal(len(response[i][item]), len(expect_response[i][item]))

                        for j in range(len(response[i][item])):
                            if expect_response[i][item][j].is_ok and not response[i][item][j].is_ok:
                                raise Exception("BatchWriteRow failed on at least one row, ErrorCode: %s ErrorMessage: %s" % (response[i][item][j].error_code, response[i][item][j].error_message))
                            self.assert_equal(response[i][item][j].is_ok, expect_response[i][item][j].is_ok)

                            if expect_response[i][item][j].is_ok:
                                self.assert_consumed(response[i][item][j].consumed, expect_response[i][item][j].consumed)
                            else:
                                self.assert_equal(response[i][item][j].error_code, expect_response[i][item][j].error_code)
                                self.assert_equal(response[i][item][j].error_message, expect_response[i][item][j].error_message)
                                self.assert_consumed(response[i][item][j].consumed, expect_response[i][item][j].consumed)
                else:
                    self.assert_false()

    def assert_time(self, res_time, expect_time, delta=60):
        if res_time == None or expect_time == None:
            self.assert_equal(res_time, expect_time)
        else:
            self.assert_equal((max(res_time, expect_time) - min(res_time, expect_time) <= delta), True)

    def assert_ReservedThroughputDetails(self, details, expect_details):
        self.assert_consumed(details.capacity_unit, expect_details.capacity_unit)
        self.assert_time(details.last_increase_time, expect_details.last_increase_time)
        self.assert_time(details.last_decrease_time, expect_details.last_decrease_time)
        self.assert_equal(details.number_of_decreases_today, expect_details.number_of_decreases_today)

    def assert_UpdateTableResponse(self, response, expect_response):
        self.assert_ReservedThroughputDetails(response.reserved_throughput_details, expect_response.reserved_throughput_details)

    def assert_TableMeta(self, response, expect_response):
        self.assert_equal(response.table_name, expect_response.table_name)
        self.assert_equal(response.schema_of_primary_key, expect_response.schema_of_primary_key)

    def assert_TableOptions(self, response, expect_response):
        self.assert_equal(response.time_to_live, expect_response.time_to_live)
        self.assert_equal(response.max_version, expect_response.max_version)
        self.assert_equal(response.max_time_deviation, expect_response.max_time_deviation)

    def assert_DescribeTableResponse(self, response, expect_capacity_unit, expect_table_meta, expect_options):
        self.assert_consumed(response.reserved_throughput_details.capacity_unit, expect_capacity_unit)
        self.assert_TableMeta(response.table_meta, expect_table_meta)
        self.assert_TableOptions(response.table_options, expect_options)

    def wait_for_capacity_unit_update(self, table_name):
        time.sleep(5)

    def wait_for_partition_load(self, table_name, instance_name=""):
        time.sleep(5)

    def get_primary_keys(self, pk_cnt, pk_type, pk_name="PK", pk_value="x"):
        pk_schema = []
        pk = []
        for i in range(pk_cnt):
            pk_schema.append(("%s%d" % (pk_name, i), pk_type))
            pk.append(("%s%d" % (pk_name, i), pk_value))

        return pk_schema, pk

    def get_row_size(self, pk_dict, column_dict):
        sum = 0
        for v in pk_dict:
            sum += len(v[0])
            if isinstance(v[1], bool):
                sum += 1
            elif isinstance(v[1], int):
                sum += 8
            elif isinstance(v[1], (six.binary_type, bytearray, six.text_type)):
                sum += len(v[1])
            else:
                raise Exception("wrong type is set in primary value")

        for v in column_dict:
            sum += len(v[0])
            if len(v) == 1:
                pass
            elif isinstance(v[1], bool):
                sum += 1
            elif isinstance(v[1], (int, float)):
                sum += 8
            elif isinstance(v[1], (six.binary_type, bytearray, six.text_type)):
                sum += len(v[1])
            else:
                raise Exception("wrong type is set in column value")
            #sum += 4
        return sum

    def sum_CU_from_row(self, pk_dict, column_dict):
        sum = self.get_row_size(pk_dict, column_dict)
        return int(math.ceil(sum * 1.0 / 4096))

    def _create_table_with_4_pk(self, table_name):
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING'),
            ('PK2', 'STRING'), ('PK3', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(
            restriction.MaxReadWriteCapacityUnit,
            restriction.MaxReadWriteCapacityUnit
        ))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

    def _create_maxsize_row(self, pk_value = 'V'):
        """创建table+生成size恰好为max的pk dict和column dict"""
        pks = []
        for i in range(0, 4):
            pk = 'PK' + str(i)
            value = pk_value * (restriction.MaxPKStringValueLength - len(pk))
            pks.append((pk, value))

        column_size = restriction.MaxPKStringValueLength * 4
        column_n = restriction.MaxColumnDataSizeForRow / column_size - 1
        columns = []
        for i in range(0, column_n):
            key = 'C' + str(i)
            value = 'V' * (column_size - len(key))
            columns.append((key,  value))
        return pks, columns

    def get_python_version(self):
        if isinstance(sys.version_info, tuple):
            python_version = '%s_%s_%s' % (sys.version_info[0], sys.version_info[1], sys.version_info[2])
        else:
            python_version = '%s_%s_%s' % (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
        return python_version


