# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
from tablestore.metadata import *
import time

table_name = 'python_sdk_5'

def create_table(client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_option, reserved_throughput)
    print ('Table has been created.')

def delete_table(client):
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

def put_row(client):
    primary_key = [('gid',1), ('uid',"101")]
    attribute_columns = [('name','John'), ('mobile',15100000000), ('address','China'), ('age',20)]
    row = Row(primary_key, attribute_columns)
    condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST) # Expect not exist: put it into table only when this row is not exist.
    consumed, return_row = client.put_row(table_name, row)
    print ('Write succeed, consume %s write cu.' % consumed.write)

def delete_row(client):
    primary_key = [('gid',1), ('uid','101')]
    row = Row(primary_key)
    condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("age", 25, ComparatorType.LESS_THAN))
    consumed, return_row = client.delete_row(table_name, row, condition) 
    print ('Delete succeed, consume %s write cu.' % consumed.write)

def get_row(client):
    primary_key = [('gid',1), ('uid',"101")]
    columns_to_get = ['name', 'address', 'age'] # given a list of columns to get, or empty list if you want to get entire row.
    consumed, return_row, next_token = client.get_row(table_name, primary_key, columns_to_get, None, 1)
    print ('Read succeed, consume %s read cu.' % consumed.read)

    if return_row is not None:
        print ('Value of attribute: %s' % return_row.attribute_columns)

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass
    create_table(client)

    time.sleep(3) # wait for table ready

    put_row(client)
    print ('#### row before delete ####')
    get_row(client)
    delete_row(client)
    print ('#### row after delete ####')
    get_row(client)

    delete_table(client)
