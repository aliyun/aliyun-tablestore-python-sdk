# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time

table_name_1 = 'OTSBatchGetRowSimpleExample_1'
table_name_2 = 'OTSBatchGetRowSimpleExample_2'

def create_table(client, table_name):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_option, reserved_throughput)
    print ('Table has been created.')

def delete_table(client, table_name):
    try:
        client.delete_table(table_name)
        print ('Table \'%s\' has been deleted.' % table_name)
    except:
        pass

def put_row(client, table_name):
    for i in range(0, 10):
        primary_key = [('gid',i), ('uid',i+1)]
        attribute_columns = [('name','John'), ('mobile',i), ('address','China'), ('age',i)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST) # Expect not exist: put it into table only when this row is not exist.
        consumed, return_row = client.put_row(table_name, row, condition)
        print (u'Write succeed, consume %s write cu.' % consumed.write)

def batch_get_row(client):
    # try to get rows from two different tables
    columns_to_get = ['name', 'mobile', 'address', 'age']
    rows_to_get = []
    for i in range(0, 10):
        primary_key = [('gid',i), ('uid',i+1)]
        rows_to_get.append(primary_key)

    cond = CompositeColumnCondition(LogicalOperator.AND)
    cond.add_sub_condition(SingleColumnCondition("name", "John", ComparatorType.EQUAL))
    cond.add_sub_condition(SingleColumnCondition("address", 'China', ComparatorType.EQUAL))

    request = BatchGetRowRequest()
    request.add(TableInBatchGetRowItem(table_name_1, rows_to_get, columns_to_get, cond, 1))
    request.add(TableInBatchGetRowItem(table_name_2, rows_to_get, columns_to_get, cond, 1))

    result = client.batch_get_row(request)

    print ('Result status: %s'%(result.is_all_succeed()))

    table_result_0 = result.get_result_by_table(table_name_1)
    table_result_1 = result.get_result_by_table(table_name_2)

    print ('Check first table\'s result:')
    for item in table_result_0:
        if item.is_ok:
            print ('Read succeed, PrimaryKey: %s, Attributes: %s' % (item.row.primary_key, item.row.attribute_columns))
        else:
            print ('Read failed, error code: %s, error message: %s' % (item.error_code, item.error_message))

    print ('Check second table\'s result:')
    for item in table_result_1:
        if item.is_ok:
            print ('Read succeed, PrimaryKey: %s, Attributes: %s' % (item.row.primary_key, item.row.attribute_columns))
        else:
            print ('Read failed, error code: %s, error message: %s' % (item.error_code, item.error_message))

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    delete_table(client, table_name_1)
    delete_table(client, table_name_2)

    create_table(client, table_name_1)
    create_table(client, table_name_2)

    time.sleep(3) # wait for table ready
    put_row(client, table_name_1)
    put_row(client, table_name_2)
    batch_get_row(client)
    delete_table(client, table_name_1)
    delete_table(client, table_name_2)

