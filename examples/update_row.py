# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time

table_name = 'python_sdk_4'

def create_table(client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
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

def update_row(client):
    primary_key = [('gid',1), ('uid',"101")]
    update_of_attribute_columns = {
        'PUT' : [('name','David'), ('address','Hongkong')],
        'DELETE' : [('address', None, 1488436949003)],
        'DELETE_ALL' : [('mobile'), ('age')],
        'INCREMENT' : [('counter', -1)]
    }
    row = Row(primary_key, update_of_attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("age", 20, ComparatorType.EQUAL)) # update row only when this row is exist
    consumed, return_row = client.update_row(table_name, row, condition)
    print ('Update succeed, consume %s write cu.' % consumed.write)

def get_row(client):
    primary_key = [('gid',1), ('uid','101')]
    columns_to_get = ['name', 'address', 'age', 'counter'] # given a list of columns to get, or empty list if you want to get entire row.
    consumed, return_row, next_token = client.get_row(table_name, primary_key, columns_to_get, None, 1)
    print ('Read succeed, consume %s read cu.' % consumed.read)

    print ('Value of attribute: %s' % return_row.attribute_columns)


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass
    create_table(client)

    #time.sleep(3) # wait for table ready
    put_row(client)
    print ('#### row before update ####')
    get_row(client)
    update_row(client)
    print ('#### row after update ####')
    get_row(client)
    delete_table(client)

