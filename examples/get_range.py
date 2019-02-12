# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time

table_name = 'OTSGetRangeSimpleExample'

def create_table(client):
    schema_of_primary_key = [('uid', 'INTEGER'), ('gid', 'BINARY')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_option, reserved_throughput)
    print ('Table has been created.')

def delete_table(client):
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

def put_row(client):
    for i in range(0, 100):
        primary_key = [('uid',i), ('gid', bytearray(str(i+1), 'utf-8'))]
        attribute_columns = [('name','John'), ('mobile',i), ('address','China'), ('age',i)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE) # Expect not exist: put it into table only when this row is not exist.
        consumed, return_row = client.put_row(table_name, row, condition)
        print ('Write succeed, consume %s write cu.' % consumed.write)

def get_range(client):
    '''
        Scan table to get all the rows.
        It will not return you all once, you should continue read from next start primary key till next start primary key is None.
    '''
    inclusive_start_primary_key = [('uid',INF_MIN), ('gid',INF_MIN)]
    exclusive_end_primary_key = [('uid',INF_MAX), ('gid',INF_MAX)]
    columns_to_get = []
    limit = 90

    cond = CompositeColumnCondition(LogicalOperator.AND)
    cond.add_sub_condition(SingleColumnCondition("address", 'China', ComparatorType.EQUAL))
    cond.add_sub_condition(SingleColumnCondition("age", 50, ComparatorType.LESS_THAN))

    consumed, next_start_primary_key, row_list, next_token  = client.get_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get,
                limit,
                column_filter = cond,
                max_version = 1
    )

    all_rows = []
    all_rows.extend(row_list)
    while next_start_primary_key is not None:
        inclusive_start_primary_key = next_start_primary_key
        consumed, next_start_primary_key, row_list, next_token = client.get_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, limit,
                column_filter = cond,
                max_version = 1
        )
        all_rows.extend(row_list)
        print ('Read succeed, consume %s read cu.' % consumed.read)

    for row in all_rows:
        print (row.primary_key, row.attribute_columns)
    print ('Total rows: ', len(all_rows))

def xget_range(client):
    '''
        You can easily scan the range use xget_range, without handling next start primary key.
    '''
    consumed_counter = CapacityUnit(0, 0)
    inclusive_start_primary_key = [('uid',INF_MIN), ('gid',INF_MIN)]
    exclusive_end_primary_key = [('uid',INF_MAX), ('gid',INF_MAX)]

    cond = CompositeColumnCondition(LogicalOperator.AND)
    cond.add_sub_condition(SingleColumnCondition("address", 'China', ComparatorType.EQUAL))
    cond.add_sub_condition(SingleColumnCondition("age", 50, ComparatorType.GREATER_EQUAL))

    columns_to_get = []
    range_iter = client.xget_range(
                table_name, Direction.FORWARD,
                inclusive_start_primary_key, exclusive_end_primary_key,
                consumed_counter, columns_to_get, 100,
                column_filter = cond, max_version = 1
    )

    total_rows = 0
    for row in range_iter:
        print (row.primary_key, row.attribute_columns)
        total_rows += 1

    print ('Total rows:', total_rows)

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass
    create_table(client)

    time.sleep(3) # wait for table ready
    put_row(client)
    get_range(client)
    xget_range(client)
    delete_table(client)

