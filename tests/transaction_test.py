# -*- coding: utf8 -*-

import unittest
from lib.api_test_base import APITestBase
from tablestore import *
from tablestore.error import *
import time
import logging


class TransactionTest(APITestBase):
    # Table support transaction must be set in advance, we can't create and use transaction immediately!
    TABLE_NAME = "TransactionTable"

    """TransactionTest"""

    def test_put_row_abort(self):  # test getRow with transaction at the same time
        """测试UpdateRow的事务，调用StartLocalTransaction API, 然后Abort掉"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]
        new_attribute_columns = [('value', 'new value')]
        columns_to_get = ['value']

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # put row with transaction_id
        row = Row(primary_key, new_attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition, None, transaction_id)

        # assert value before abort
        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1, transaction_id=transaction_id
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'new value')

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

        # abort transaction
        self.client_test.abort_transaction(transaction_id)

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

    def test_put_row_commit(self):  # test getRow with transaction at the same time
        """测试UpdateRow的事务，调用StartLocalTransaction API, 然后Commit提交"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]
        new_attribute_columns = [('value', 'new value')]
        columns_to_get = ['value']

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # put row with transaction_id
        row = Row(primary_key, new_attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition, None, transaction_id)

        # assert value before abort
        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1, transaction_id=transaction_id
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'new value')

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

        # abort transaction
        self.client_test.commit_transaction(transaction_id)

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'new value')

    def test_update_row_abort(self):  # test getRow with transaction at the same time
        """测试UpdateRow的事务，调用StartLocalTransaction API, 然后Abort掉"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]
        update_of_attribute_columns = {'PUT': [('value', 'new value')]}
        columns_to_get = ['value']

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # update row with transaction_id
        update_row = Row(primary_key, update_of_attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.update_row(table_name, update_row, condition, None, transaction_id)

        # assert value before abort
        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1, transaction_id=transaction_id
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'new value')

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

        # abort transaction
        self.client_test.abort_transaction(transaction_id)

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

    def test_update_row_commit(self):  # test getRow with transaction at the same time
        """测试UpdateRow的事务，调用StartLocalTransaction API, 然后Commit提交"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]
        update_of_attribute_columns = {'PUT': [('value', 'new value')]}
        columns_to_get = ['value']

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # update row with transaction_id
        update_row = Row(primary_key, update_of_attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.update_row(table_name, update_row, condition, None, transaction_id)

        # assert value before abort
        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1, transaction_id=transaction_id
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'new value')

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

        # abort transaction
        self.client_test.commit_transaction(transaction_id)

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'new value')

    def test_batch_write_row_abort(self):  # test getRow with transaction at the same time
        """测试BatchWriteRow的事务，调用StartLocalTransaction API, 然后Abort掉"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]
        update_of_attribute_columns = {'put': [('value', 'batch value')]}
        columns_to_get = ['value']

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # batch write row with transaction_id
        put_row_items = []

        row = Row(primary_key, update_of_attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        item = UpdateRowItem(row, condition)
        put_row_items.append(item)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, put_row_items))
        request.set_transaction_id(transaction_id)
        self.client_test.batch_write_row(request)

        # assert value before abort
        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1, transaction_id=transaction_id
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'batch value')

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

        # abort transaction
        self.client_test.abort_transaction(transaction_id)

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

    def test_batch_write_row_commit(self):  # test getRow with transaction at the same time
        """测试BatchWriteRow的事务，调用StartLocalTransaction API, 然后Commit提交"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]
        update_of_attribute_columns = {'put': [('value', 'batch value')]}
        columns_to_get = ['value']

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # batch write row with transaction
        put_row_items = []

        row = Row(primary_key, update_of_attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        item = UpdateRowItem(row, condition)
        put_row_items.append(item)

        request = BatchWriteRowRequest()
        request.add(TableInBatchWriteRowItem(table_name, put_row_items))
        request.set_transaction_id(transaction_id)
        self.client_test.batch_write_row(request)

        # assert value before abort
        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1, transaction_id=transaction_id
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'batch value')

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'origin value')

        # abort transaction
        self.client_test.commit_transaction(transaction_id)

        consumed, return_row, next_token = self.client_test.get_row(
            table_name, primary_key, columns_to_get, None, 1
        )
        self.assert_equal(return_row.attribute_columns[0][1], 'batch value')

    def test_delete_row_abort(self):  # test getRange with transaction at the same time
        """测试DeleteRow的事务，调用StartLocalTransaction API, 然后Abort掉"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # delete row with transaction_id
        row = Row(primary_key)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.delete_row(table_name, row, condition, transaction_id=transaction_id)

        # assert value before abort
        inclusive_start_primary_key = [('PK0', 1), ('PK1', INF_MIN)]
        exclusive_end_primary_key = [('PK0', 1), ('PK1', INF_MAX)]
        columns_to_get = []
        limit = 1

        consumed, next_start_primary_key, row_list, next_token = self.client_test.get_range(
            table_name, Direction.FORWARD,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get,
            limit,
            column_filter=None,
            max_version=1,
            transaction_id=transaction_id
        )
        self.assert_equal(len(row_list), 0)

        consumed, next_start_primary_key, row_list, next_token = self.client_test.get_range(
            table_name, Direction.FORWARD,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get,
            limit,
            column_filter=None,
            max_version=1
        )
        self.assert_equal(len(row_list), 1)

        # abort transaction
        self.client_test.abort_transaction(transaction_id)

        consumed, next_start_primary_key, row_list, next_token = self.client_test.get_range(
            table_name, Direction.FORWARD,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get,
            limit,
            column_filter=None,
            max_version=1
        )
        self.assert_equal(len(row_list), 1)

    def test_delete_row_commit(self):  # test getRange with transaction at the same time
        """测试DeleteRow的事务，调用StartLocalTransaction API, 然后Commit提交"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        attribute_columns = [('value', 'origin value')]
        key = [('PK0', 1)]

        # init data
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.put_row(table_name, row, condition)

        # start transaction
        transaction_id = self.client_test.start_local_transaction(table_name, key)

        # delete row with transaction_id
        row = Row(primary_key)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.client_test.delete_row(table_name, row, condition, transaction_id=transaction_id)

        # assert value before abort
        inclusive_start_primary_key = [('PK0', 1), ('PK1', INF_MIN)]
        exclusive_end_primary_key = [('PK0', 1), ('PK1', INF_MAX)]
        columns_to_get = []
        limit = 1

        consumed, next_start_primary_key, row_list, next_token = self.client_test.get_range(
            table_name, Direction.FORWARD,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get,
            limit,
            column_filter=None,
            max_version=1,
            transaction_id=transaction_id
        )
        self.assert_equal(len(row_list), 0)

        consumed, next_start_primary_key, row_list, next_token = self.client_test.get_range(
            table_name, Direction.FORWARD,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get,
            limit,
            column_filter=None,
            max_version=1
        )
        self.assert_equal(len(row_list), 1)

        # commit transaction
        self.client_test.commit_transaction(transaction_id)

        consumed, next_start_primary_key, row_list, next_token = self.client_test.get_range(
            table_name, Direction.FORWARD,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get,
            limit,
            column_filter=None,
            max_version=1
        )
        self.assert_equal(len(row_list), 0)

    def test_invalid_transaction_id(self):
        """Fail测试: 无效事务ID"""

        table_name = TransactionTest.TABLE_NAME
        primary_key = [('PK0', 1), ('PK1', 'transaction')]
        new_attribute_columns = [('value', 'new value')]

        # start transaction
        transaction_id = "not existed transaction id"

        # put row with invalid transaction_id
        try:
            row = Row(primary_key, new_attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.client_test.put_row(table_name, row, condition, None, transaction_id)
        except OTSServiceError as e:
            self.assert_error(e, 400, "OTSParameterInvalid", "TransactionID is invalid.")


if __name__ == '__main__':
    unittest.main()
