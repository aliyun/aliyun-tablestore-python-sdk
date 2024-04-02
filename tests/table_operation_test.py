# -*- coding: utf8 -*-

import unittest
import time
from lib.api_test_base import APITestBase
from lib.api_test_base import get_no_retry_client
from tablestore import *
from tablestore.error import *

class TableOperationTest(APITestBase):

    """表级别操作测试"""

    def test_delete_existing_table(self):
        """删除一个存在的表，期望成功, list_table()确认表已经删除, describe_table()返回异常OTSObjectNotExist"""
        table_name = 'table_test_delete_existing' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.client_test.delete_table(table_name)
        self.assert_equal(False, table_name in self.client_test.list_table())

        try:
            self.client_test.describe_table(table_name)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 404, "OTSObjectNotExist", "Requested table does not exist.")

    def test_create_table_already_exist(self):
        """创建一个表，表名与现有表重复，期望返回ErrorCode: OTSObjectAlreadyExist, list_table()确认没有2个重名的表"""
        table_name = 'table_test_already_exist' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_meta_new = TableMeta(table_name, [('PK2', 'STRING'), ('PK3', 'STRING')])
        try:
            self.client_test.create_table(table_meta_new, table_options, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 409, "OTSObjectAlreadyExist", "Requested table already exists.")

        table_list = self.client_test.list_table()
        self.assert_equal(1, table_list.count(table_name))

    def test_create_table_with_sequence(self):
        """创建一个表，PK的顺序的影响"""
        table_name = 'table_test_sequence' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK9', 'STRING'), ('PK1', 'INTEGER'), ('PK3', 'BINARY')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_list = self.client_test.list_table()
        self.assert_equal(1, table_list.count(table_name))

    def test_duplicate_PK_name_in_table_meta(self):
        """创建表的时候，TableMeta中有2个PK列，列名重复，期望返回OTSParameterInvalid，list_table()确认没有这个表"""
        table_name = 'table_test_duplicate_PK' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK0', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        try:
            self.client_test.create_table(table_meta, table_options, reserved_throughput)
            self.assert_false()
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "Duplicated primary key name: 'PK0'.")

        self.assert_equal(False, table_name in self.client_test.list_table())

    def test_PK_option(self):
        """测试table_option的默认值、自定义值和值更新"""
        table_name = 'table_PK_option' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'INTEGER'), ('PK2', 'INTEGER')])
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        table_options_default = TableOptions()  # 使用默认option
        self.client_test.create_table(table_meta, table_options_default, reserved_throughput)
        describe_response = self.client_test.describe_table(table_name)
        expect_options = TableOptions(-1, 1, 86400)  # 默认option为(-1, 1, 86400)
        self.assert_DescribeTableResponse(
            describe_response, reserved_throughput.capacity_unit, table_meta, expect_options)

        # 删除表
        self.client_test.delete_table(table_name)
        time.sleep(0.5)

        table_options_special = TableOptions(1200000, 2, 86401)  # 使用特殊option
        self.assert_CreateTableResult(table_name, table_meta, table_options_special, reserved_throughput)
        time.sleep(0.5)

        table_options_update = TableOptions(-1, 3, 86402)  # 测试更新option
        self.client_test.update_table(table_name, table_options_update)
        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta,
                                          table_options_update)

    def test_PK_type(self):
        """测试PK列的类型，包括STRING类型、INTEGER类型、BINARY类型和非法类型"""
        table_name = 'table_PK_type' + self.get_python_version()
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        # 合法类型
        valid_type = ['STRING', 'INTEGER', 'BINARY']
        for vt in valid_type:
            table_meta = TableMeta(table_name,
                                   [('PK0', vt), ('PK1', vt), ('PK2', vt), ('PK3', vt)])
            self.assert_CreateTableResult(table_name, table_meta, table_options, reserved_throughput)
            # 删除表
            self.client_test.delete_table(table_name)
            time.sleep(0.5)

        # 非法类型
        invalid_type = ['DOUBLE', 'BOOLEAN']
        for vt in invalid_type:
            table_meta = TableMeta(table_name,
                                   [('PK0', vt), ('PK1', vt), ('PK2', vt), ('PK3', vt)])
            try:
                self.client_test.create_table(table_meta, table_options, reserved_throughput)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", vt + " is an invalid type for the primary key.")
            except OTSClientError as e:
                self.assert_equal("primary_key_type should be one of [BINARY, INTEGER, STRING], not " + vt, str(e))
            self.assert_equal(False, table_name in self.client_test.list_table())
            time.sleep(0.5)

    def test_create_table_again(self):
        """
        创建一个表，设置CU(1, 1), 删除它，然后用同样的Name，不同的PK创建表，设置CU为(2, 2)，操作验证CU
        """
        table_name = 'table_create_again' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(1, 1))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.client_test.delete_table(table_name)

        table_meta_new = TableMeta(table_name, [('PK0_new', 'INTEGER'), ('PK1', 'STRING')])
        reserved_throughput_new = ReservedThroughput(CapacityUnit(2, 2))
        self.client_test.create_table(table_meta_new, table_options, reserved_throughput_new)
        self.wait_for_partition_load('table_create_again')

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(
            describe_response, reserved_throughput_new.capacity_unit, table_meta_new, table_options)

        pk_dict_exist = [('PK0_new', 3), ('PK1', '1')]
        pk_dict_not_exist = [('PK0_new', 5), ('PK1', '2')]
        self.check_CU_by_consuming(
            table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_new.capacity_unit)

    def test_CU_not_messed_up_with_two_tables(self):
        """创建2个表，分别设置CU为(1, 2)和(2, 1)，操作验证CU，describe_table()确认设置成功"""
        table_name_1 = 'table1_CU_mess_up_test' + self.get_python_version()
        table_meta_1 = TableMeta(table_name_1, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput_1 = ReservedThroughput(CapacityUnit(1, 2))
        table_name_2 = 'table2_CU_mess_up_test' + self.get_python_version()
        table_meta_2 = TableMeta(table_name_2, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput_2 = ReservedThroughput(CapacityUnit(2, 1))
        pk_dict_exist = [('PK0', 'a'), ('PK1', '1')]
        pk_dict_not_exist = [('PK0', 'B'), ('PK1', '2')]
        table_options = TableOptions()
        self.client_test.create_table(table_meta_1, table_options, reserved_throughput_1)
        self.client_test.create_table(table_meta_2, table_options, reserved_throughput_2)
        self.wait_for_partition_load('table1_CU_mess_up_test')
        self.wait_for_partition_load('table2_CU_mess_up_test')

        describe_response_1 = self.client_test.describe_table(table_name_1)
        self.assert_DescribeTableResponse(
            describe_response_1, reserved_throughput_1.capacity_unit, table_meta_1, table_options)
        self.check_CU_by_consuming(
            table_name_1, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_1.capacity_unit)
        describe_response_2 = self.client_test.describe_table(table_name_2)
        self.assert_DescribeTableResponse(
            describe_response_2, reserved_throughput_2.capacity_unit, table_meta_2, table_options)
        self.check_CU_by_consuming(
            table_name_2, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_2.capacity_unit)

    def test_create_table_with_CU(self):
        """创建1个表，CU是(0, 0)到(1, 1)，describe_table()确认设置成功"""
        for (i, j) in [(0, 0), (0, 1), (1, 0), (1, 1)]:
            table_name = 'table_cu_' + str(i) + '_' + str(j) + self.get_python_version()
            table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
            table_options = TableOptions()
            reserved_throughput = ReservedThroughput(CapacityUnit(i, j))
            self.client_test.create_table(table_meta, table_options, reserved_throughput)
            self.wait_for_partition_load(table_name)

            describe_response = self.client_test.describe_table(table_name)
            self.assert_DescribeTableResponse(
                describe_response, reserved_throughput.capacity_unit, table_meta, table_options)
            time.sleep(0.5)

    def _assert_index_meta(self, expect_index, actual_index):
        self.assert_equal(expect_index.index_name, actual_index.index_name)
        self.assert_equal(expect_index.primary_key_names, actual_index.primary_key_names)
        self.assert_equal(expect_index.defined_column_names, actual_index.defined_column_names)
        self.assert_equal(expect_index.index_type, actual_index.index_type)

    def test_create_table_with_secondary_index(self):
        """测试创建包含二级索引的表"""
        table_name = 'table_with_index' + self.get_python_version()
        schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
        defined_columns = [('i', 'INTEGER'), ('bool', 'BOOLEAN'), ('d', 'DOUBLE'), ('s', 'STRING'), ('b', 'BINARY')]
        table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_option = TableOptions(-1, 1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        secondary_indexes = [
            SecondaryIndexMeta('index_1', ['i', 's'], ['bool', 'b', 'd']),
        ]

        # 建表时创建二级索引
        self.client_test.create_table(table_meta, table_option, reserved_throughput, secondary_indexes)

        dtr = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(dtr, reserved_throughput.capacity_unit, table_meta, table_option)
        self.assert_equal(table_meta.defined_columns, dtr.table_meta.defined_columns)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_indexes[0], dtr.secondary_indexes[0])

    def test_create_secondary_index(self):
        """测试创建二级索引"""
        table_name = 'table_with_index' + self.get_python_version()
        schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
        defined_columns = [('i', 'INTEGER'), ('bool', 'BOOLEAN'), ('d', 'DOUBLE'), ('s', 'STRING'), ('b', 'BINARY')]
        table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_option = TableOptions(-1, 1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))

        # 建表
        self.client_test.create_table(table_meta, table_option, reserved_throughput)

        # 创建二级索引，include_base_data设为False
        secondary_index_meta = SecondaryIndexMeta('index_1', ['s', 'b'], ['i'])
        self.client_test.create_secondary_index(table_name, secondary_index_meta, False)

        dtr = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(dtr, reserved_throughput.capacity_unit, table_meta, table_option)
        self.assert_equal(table_meta.defined_columns, dtr.table_meta.defined_columns)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_index_meta, dtr.secondary_indexes[0])

        # 删除索引
        self.client_test.delete_secondary_index(table_name, 'index_1')
        dtr = self.client_test.describe_table(table_name)
        self.assert_equal(0, len(dtr.secondary_indexes))

        def put_row(pk, cols):
            row = Row(pk, cols)
            condition = Condition('EXPECT_NOT_EXIST')
            _, _ = self.client_test.put_row(table_name, row, condition)
            _, rows, _ = self.client_test.get_row(table_name, pk, max_version=1)
            self.assert_equal(rows.primary_key, pk)
            self.assert_columns(rows.attribute_columns, cols)
        # 插入数据
        primary_key = [('gid', 0), ('uid', '0')]
        attribute_columns = [('i', 0), ('bool', True), ('d', 123.0), ('s', 'test1'), ('b', bytearray(1))]
        put_row(primary_key, attribute_columns)
        primary_key = [('gid', 0), ('uid', '1')]
        attribute_columns = [('i', 1), ('bool', True), ('d', 321.0), ('s', 'test2'), ('b', bytearray(2))]
        put_row(primary_key, attribute_columns)

        # 创建二级索引，测试include_base_data设为True
        current_time = int(time.time()*1000)
        index_name = 'index_' + str(current_time)
        secondary_index_meta = SecondaryIndexMeta(
            index_name, ['gid', 's'], ['bool', 'd'])
        self.client_test.create_secondary_index(table_name, secondary_index_meta, True)

        time.sleep(1)  # 等待索引建立
        dtr = self.client_test.describe_table(table_name)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_index_meta, dtr.secondary_indexes[0])

        # 等待存量数据的同步，这个时间会比较久
        columns_to_get = ['bool', 'd']
        primary_key = [('gid', 0), ('s', 'test2'), ('uid', '1')]
        no_retry_client = get_no_retry_client()
        retry_times = 0
        max_retry_times = 20
        while retry_times < max_retry_times:
            try:
                _, return_row, _ = no_retry_client.get_row(
                    index_name, primary_key, columns_to_get=columns_to_get, max_version=1)
            except OTSServiceError as e:
                # 该错误message表示存量数据还在同步中，故忽略
                # 若不是该错误message则抛出异常
                if e.message != 'Disallow read index table in building base state':
                    raise e
                retry_times += 1
                self.assertLess(retry_times, max_retry_times, 'exceed retry times -- ' + str(e))
                time.sleep(10)
                continue
            self.assert_equal(return_row.primary_key, primary_key)
            expect_cols = [('bool', True), ('d', 321.0)]
            self.assert_columns(return_row.attribute_columns, expect_cols)
            break

if __name__ == '__main__':
    unittest.main()

