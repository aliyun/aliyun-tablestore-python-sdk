# -*- coding: utf8 -*-

from example_config import *

from tablestore import *
from tablestore.retry import WriteRetryPolicy

import time

table_name = 'OTSPutRowSimpleExample'

def create_table(client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
    print ('Table has been created.')

def delete_table(client):
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

def put_row(client):
    primary_key = [('gid',1), ('uid',101)]
    attribute_columns = [('name','萧峰'), ('mobile',15100000000), ('address', bytearray('China', 'utf-8')),
                         ('female', False), ('age', 29.7)]
    row = Row(primary_key, attribute_columns)

    condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST, SingleColumnCondition("age", 20, ComparatorType.EQUAL))
    consumed, return_row = client.put_row(table_name, row, condition)
    print (u'Write succeed, consume %s write cu.' % consumed.write)

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE, sts_token = OTS_STS_TOKEN, retry_policy = WriteRetryPolicy())
    try:
        delete_table(client)
    except:
        pass
    create_table(client)

    time.sleep(3) # wait for table ready

    put_row(client)
    delete_table(client)

