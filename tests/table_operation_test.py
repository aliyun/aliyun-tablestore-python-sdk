# -*- coding: utf8 -*-

import unittest
from lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging

class TableOperationTest(APITestBase):

    """表级别操作测试"""

    def test_delete_existing_table(self):
        """删除一个存在的表，期望成功, list_table()确认表已经删除, describe_table()返回异常OTSObjectNotExist"""
        time.sleep(1) # to avoid too frequently table operation
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
        time.sleep(1) # to avoid too frequently table operation
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
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_test_sequence' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK9', 'STRING'), ('PK1', 'INTEGER'), ('PK3', 'BINARY')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        table_list = self.client_test.list_table()
        self.assert_equal(1, table_list.count(table_name))


    def test_duplicate_PK_name_in_table_meta(self):
        """创建表的时候，TableMeta中有2个PK列，列名重复，期望返回OTSParameterInvalid，list_table()确认没有这个表"""
        time.sleep(1) # to avoid too frequently table operation
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

    def test_PK_option_DEFAULT(self):
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_option_default' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'INTEGER'), ('PK2', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        describe_response = self.client_test.describe_table(table_name)
        expect_options = TableOptions(-1, 1, 86400)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, expect_options)

    def test_PK_option_SPECIAL(self):
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_option_default' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'INTEGER'), ('PK2', 'INTEGER')])
        table_options = TableOptions(1200000, 2, 86401)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_PK_option_UPDATE(self):
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_option_default' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'INTEGER'), ('PK2', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

        time.sleep(60)
        table_options = TableOptions(1200000, 2, 86401)
        self.client_test.update_table(table_name, table_options)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_PK_type_STRING(self):
        """创建表的时候，TableMeta中有4个PK列，都为STRING类型，期望正常，describe_table()获取信息与创表参数一致"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_type_STRING' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING'), ('PK2', 'STRING'), ('PK3', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_PK_type_INTEGER(self):
        """创建表的时候，TableMeta中有4个PK列，都为INTEGER类型，期望正常，describe_table()获取信息与创表参数一致"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_type_INTEGER' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'INTEGER'), ('PK2', 'INTEGER'), ('PK3', 'INTEGER')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_PK_type_BINARY(self):
        """创建表的时候，TableMeta中有4个PK列，都为BINARY类型，期望正常，describe_table()获取信息与创表参数一致"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_type_BINARY' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'BINARY'), ('PK1', 'BINARY'), ('PK2', 'BINARY'), ('PK3', 'BINARY')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_PK_type_invalid(self):
        """测试创建表时，第1，2，3，4个PK列type分别为DOUBLE, BOOLEAN, BINARY，期望返回OTSParameterInvalid, list_table()确认创建失败"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_PK_type_invalid' + self.get_python_version()
        pk_list = []
        er_col = []
        pk_list.append([('PK0', 'DOUBLE'), ('PK1', 'STRING'), ('PK2', 'STRING'), ('PK3', 'STRING')])
        er_col.append(0)
        pk_list.append([('PK0', 'STRING'), ('PK1', 'DOUBLE'), ('PK2', 'STRING'), ('PK3', 'STRING')])
        er_col.append(1)
        pk_list.append([('PK0', 'STRING'), ('PK1', 'STRING'), ('PK2', 'DOUBLE'), ('PK3', 'STRING')])
        er_col.append(2)
        pk_list.append([('PK0', 'STRING'), ('PK1', 'STRING'), ('PK2', 'STRING'), ('PK3', 'DOUBLE')])
        er_col.append(3)
        for pk_schema, i in zip(pk_list, er_col):
            table_meta = TableMeta(table_name, pk_schema)
            table_options = TableOptions()
            reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
            try:
                self.client_test.create_table(table_meta, table_options, reserved_throughput)
                self.assert_false()
            except OTSServiceError as e:
                self.assert_error(e, 400, "OTSParameterInvalid", pk_schema[i][1] + " is an invalid type for the primary key.")
            except OTSClientError as e:
                self.assert_equal("primary_key_type should be one of [BINARY, INTEGER, STRING], not DOUBLE", str(e))
            self.assert_equal(False, table_name in self.client_test.list_table())

    def test_create_table_again(self):
        """创建一个表，设置CU(1, 1), 删除它，然后用同样的Name，不同的PK创建表，设置CU为(2, 2)，期望成功，describe_table()获取信息与创建表一致，CU为(2,2)，操作验证CU"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_create_again' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'INTEGER'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)

        self.client_test.delete_table(table_name)

        table_meta_new = TableMeta(table_name, [('PK0_new', 'INTEGER'), ('PK1', 'STRING')])
        reserved_throughput_new = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta_new, table_options, reserved_throughput_new)
        self.wait_for_partition_load('table_create_again')

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput_new.capacity_unit, table_meta_new, table_options)

        pk_dict_exist = [('PK0_new',3), ('PK1','1')]
        pk_dict_not_exist = [('PK0_new',5), ('PK1','2')]
        self.check_CU_by_consuming(table_name, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_new.capacity_unit)

    def test_CU_doesnot_messed_up_with_two_tables(self):
        """创建2个表，分别设置CU为(1, 2)和(2, 1)，操作验证CU，describe_table()确认设置成功"""
        time.sleep(1) # to avoid too frequently table operation
        table_name_1 = 'table1_CU_mess_up_test' + self.get_python_version()
        table_meta_1 = TableMeta(table_name_1, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput_1 = ReservedThroughput(CapacityUnit(0, 0))
        table_name_2 = 'table2_CU_mess_up_test' + self.get_python_version()
        table_meta_2 = TableMeta(table_name_2, [('PK0', 'STRING'), ('PK1', 'STRING')])
        reserved_throughput_2 = ReservedThroughput(CapacityUnit(0, 0))
        pk_dict_exist = [('PK0','a'), ('PK1','1')]
        pk_dict_not_exist = [('PK0','B'), ('PK1','2')]
        table_options = TableOptions()
        self.client_test.create_table(table_meta_1, table_options, reserved_throughput_1)
        self.client_test.create_table(table_meta_2, table_options, reserved_throughput_2)
        self.wait_for_partition_load('table1_CU_mess_up_test')
        self.wait_for_partition_load('table2_CU_mess_up_test')

        describe_response_1 = self.client_test.describe_table(table_name_1)
        self.assert_DescribeTableResponse(describe_response_1, reserved_throughput_1.capacity_unit, table_meta_1, table_options)
        self.check_CU_by_consuming(table_name_1, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_1.capacity_unit)
        describe_response_2 = self.client_test.describe_table(table_name_2)
        self.assert_DescribeTableResponse(describe_response_2, reserved_throughput_2.capacity_unit, table_meta_2, table_options)
        self.check_CU_by_consuming(table_name_2, pk_dict_exist,  pk_dict_not_exist, reserved_throughput_2.capacity_unit)

    def test_create_table_with_CU_0_0(self):
        """创建1个表，CU是(0, 0)，describe_table()确认设置成功"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_cu_0_0' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_create_table_with_CU_0_1(self):
        """创建1个表，CU是(0, 1)，describe_table()确认设置成功"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_cu_0_1' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_create_table_with_CU_1_0(self):
        """创建1个表，CU是(1, 0)，describe_table()确认设置成功"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_cu_1_0' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def test_create_table_with_CU_1_1(self):
        """创建1个表，CU是(1, 1)，describe_table()确认设置成功"""
        time.sleep(1) # to avoid too frequently table operation
        table_name = 'table_cu_1_1' + self.get_python_version()
        table_meta = TableMeta(table_name, [('PK0', 'STRING'), ('PK1', 'STRING')])
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.client_test.create_table(table_meta, table_options, reserved_throughput)
        self.wait_for_partition_load(table_name)

        describe_response = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(describe_response, reserved_throughput.capacity_unit, table_meta, table_options)

    def _assert_index_meta(self, expect_index, actual_index):
        self.assert_equal(expect_index.index_name, actual_index.index_name)
        self.assert_equal(expect_index.primary_key_names, actual_index.primary_key_names)
        self.assert_equal(expect_index.defined_column_names, actual_index.defined_column_names)
        self.assert_equal(expect_index.index_type, actual_index.index_type)

    def test_create_table_with_secondary_index(self):
        time.sleep(1)

        table_name = 'table_with_index' + self.get_python_version()
        schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
        defined_columns = [('i', 'INTEGER'), ('bool', 'BOOLEAN'), ('d', 'DOUBLE'), ('s', 'STRING'), ('b', 'BINARY')]
        table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_option = TableOptions(-1, 1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        secondary_indexes = [
             SecondaryIndexMeta('index_1', ['i', 's'], ['gid', 'uid', 'bool', 'b', 'd']),
             ]
        self.client_test.create_table(table_meta, table_option, reserved_throughput, secondary_indexes)

        index_meta = SecondaryIndexMeta('index_2', ['s', 'b'], ['gid', 'uid', 'i'])
        self.client_test.create_secondary_index(table_name, index_meta)

        dtr = self.client_test.describe_table(table_name)
        self.assert_DescribeTableResponse(dtr, reserved_throughput.capacity_unit, table_meta, table_option)
        self.assert_equal(table_meta.defined_columns, dtr.table_meta.defined_columns)

        self.assert_equal(2, len(dtr.secondary_indexes))
        self._assert_index_meta(secondary_indexes[0], dtr.secondary_indexes[0])
        self._assert_index_meta(index_meta, dtr.secondary_indexes[1])

        self.client_test.delete_secondary_index(table_name, 'index_1')
        dtr = self.client_test.describe_table(table_name)
        self.assert_equal(1, len(dtr.secondary_indexes))
        self._assert_index_meta(index_meta, dtr.secondary_indexes[0])

if __name__ == '__main__':
    unittest.main()

