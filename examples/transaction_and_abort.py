# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time

table_name = 'TransactionTable'


def start_transaction():
    key = [('PK0', 1)]
    transaction_id = client.start_local_transaction(table_name, key)
    print ('Value of transaction id: %s' % transaction_id)

    return transaction_id


def put_row():
    primary_key = [('PK0', 1), ('PK1', 'transaction')]
    attribute_columns = [('value', 'origion value')]
    row = Row(primary_key, attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE)
    consumed, return_row = client.put_row(table_name, row, condition)
    print ('Write succeed, consume %s write cu.' % consumed.write)


def get_row(transaction_id):
    primary_key = [('PK0', 1), ('PK1', 'transaction')]
    columns_to_get = ['value']

    consumed, return_row, next_token = client.get_row(
        table_name, primary_key, columns_to_get, None, 1, None, None, None, None, transaction_id
    )

    for att in return_row.attribute_columns:
        print ('\tname:%s\tvalue:%s' % (att[0], att[1]))


def update_row(transaction_id):
    primary_key = [('PK0', 1), ('PK1', 'transaction')]
    update_of_attribute_columns = {
        'PUT': [('value', 'new value')]
    }
    row = Row(primary_key, update_of_attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE)
    consumed, return_row = client.update_row(table_name, row, condition, None, transaction_id)
    print ('Update succeed, consume %s write cu.' % consumed.write)


def get_range(transaction_id):
    inclusive_start_primary_key = [('PK0', 1), ('PK1', INF_MIN)]
    exclusive_end_primary_key = [('PK0', 1), ('PK1', INF_MAX)]
    columns_to_get = []
    limit = 1

    consumed, next_start_primary_key, row_list, next_token  = client.get_range(
        table_name, Direction.FORWARD,
        inclusive_start_primary_key, exclusive_end_primary_key,
        columns_to_get,
        limit,
        column_filter=None,
        max_version=1,
        transaction_id=transaction_id
    )

    all_rows = []
    all_rows.extend(row_list)

    for row in all_rows:
        print (row.primary_key, row.attribute_columns)
    print ('Total rows: ', len(all_rows))


def batch_write_row(transaction_id):
    put_row_items = []

    primary_key = [('PK0', 1), ('PK1', 'transaction')]
    attribute_columns = {'put': [('batch', 'batch value')]}
    row = Row(primary_key, attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE)
    item = UpdateRowItem(row, condition)
    put_row_items.append(item)

    request = BatchWriteRowRequest()
    request.add(TableInBatchWriteRowItem(table_name, put_row_items))
    request.set_transaction_id(transaction_id)

    result = client.batch_write_row(request)

    print ('Result status: %s' % (result.is_all_succeed()))
    print ('check first table\'s put results:')

    succ, fail = result.get_update()
    for item in succ:
        print ('Update succeed, consume %s write cu.' % item.consumed.write)
    for item in fail:
        print ('Update failed, error code: %s, error message: %s' % (item.error_code, item.error_message))


def abort_transaction(transaction_id):
    client.abort_transaction(transaction_id)


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)

    put_row()
    get_row(None)

    transaction_id = start_transaction()
    update_row(transaction_id)
    batch_write_row(transaction_id)

    print ('Get Origin Value')
    get_row(None)
    print ('Get Transaction Value')
    get_row(transaction_id)

    get_range(None)
    get_range(transaction_id)
    abort_transaction(transaction_id)

    print ('Get Final Value [Abort]')
    get_row(None)
