# -*- coding: utf8 -*-

from example_config import *

from tablestore import *
import tablestore.protobuf.table_store_pb2 as pb2
import time

table_name = 'OTSPkAutoIncrSimpleExample'

def create_table(client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER', PK_AUTO_INCR)]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
    print ('Table has been created.')

def describe_table(client):
    describe_response = client.describe_table(table_name)
    print ('TableName: %s' % describe_response.table_meta.table_name)
    print ('PrimaryKey: %s' % describe_response.table_meta.schema_of_primary_key)

def delete_table(client):
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

def put_row(client):
    primary_key = [('gid',1), ('uid', PK_AUTO_INCR)]
    attribute_columns = [('name','John'), ('mobile',15100000000), ('address','China'), ('age',20)]
    row = Row(primary_key, attribute_columns)

    # Expect not exist: put it into table only when this row is not exist.
    row.attribute_columns = [('name','John'), ('mobile',15100000000), ('address','China'), ('age',25)]
    consumed, return_row = client.put_row(table_name, row)
    print ('Write succeed, consume %s write cu.' % consumed.write)

    consumed, return_row = client.put_row(table_name, row, return_type = ReturnType.RT_PK)
    print ('Write succeed, consume %s write cu.' % consumed.write)
    print ('Primary key:%s' % return_row.primary_key)

def batch_write_row(client):
    put_row_items = []
    for i in range(0, 10):
        primary_key = [('gid',i), ('uid', PK_AUTO_INCR)]
        attribute_columns = [('name','somebody'+str(i)), ('address','somewhere'+str(i)), ('age',i)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        item = PutRowItem(row, condition, return_type = ReturnType.RT_PK)
        put_row_items.append(item)

    request = BatchWriteRowRequest()
    request.add(TableInBatchWriteRowItem(table_name, put_row_items))
    result = client.batch_write_row(request)

    print ('Result status: %s'%(result.is_all_succeed()))
    print ('check first table\'s put results:')
    succ, fail = result.get_put()
    for item in succ:
        print ('Put succeed, primary key:%s.' % item.row.primary_key)
    for item in fail:
        print ('Put failed, error code: %s, error message: %s' % (item.error_code, item.error_message))


def get_range(client):
    inclusive_start_primary_key = [('gid',INF_MIN), ('uid',INF_MIN)]
    exclusive_end_primary_key = [('gid',INF_MAX), ('uid',INF_MAX)]
    columns_to_get = []
    limit = 90

    consumed, next_start_primary_key, row_list, next_token  = client.get_range(
                table_name, Direction.FORWARD, 
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 
                limit, 
                column_filter = None,
                max_version = 1
    )
    for row in row_list:
        print (row.primary_key)
    

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass
    create_table(client)

    time.sleep(3) # wait for table ready

    describe_table(client)
    put_row(client)
    batch_write_row(client)
    get_range(client)
    delete_table(client)

