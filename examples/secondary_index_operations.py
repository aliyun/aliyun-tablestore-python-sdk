# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json

table_name = 'SecondaryIndexOperationExample'
index_name_1 = 'index1'
index_name_2 = 'index2'

def create_table(client):
    print ('Begin CreateTable')
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
    defined_columns = [('i', 'INTEGER'), ('bool', 'BOOLEAN'), ('d', 'DOUBLE'), ('s', 'STRING'), ('b', 'BINARY')]
    table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
    table_option = TableOptions(-1, 1)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    secondary_indexes = [
            SecondaryIndexMeta(index_name_1, ['i', 's'], ['bool', 'b', 'd']),
            ]
    client.create_table(table_meta, table_option, reserved_throughput, secondary_indexes)
    print ('Table has been created.')

def create_index(client):
    print ('Begin CreateIndex')
    index_meta = SecondaryIndexMeta(index_name_2, ['i', 's'], ['bool', 'b', 'd'])
    client.create_secondary_index(table_name, index_meta)
    print ('Index has been created.')

def describe_table(client):
    print ('Begin DescribeTable')
    describe_response = client.describe_table(table_name)
    print ('TableName: %s' % describe_response.table_meta.table_name)
    print ('PrimaryKey: %s' % describe_response.table_meta.schema_of_primary_key)
    print ('Reserved read throughput: %s' % describe_response.reserved_throughput_details.capacity_unit.read)
    print ('Reserved write throughput: %s' % describe_response.reserved_throughput_details.capacity_unit.write)
    print ('Last increase throughput time: %s' % describe_response.reserved_throughput_details.last_increase_time)
    print ('Last decrease throughput time: %s' % describe_response.reserved_throughput_details.last_decrease_time)
    print ('table options\'s time to live: %s' % describe_response.table_options.time_to_live)
    print ('table options\'s max version: %s' % describe_response.table_options.max_version)
    print ('table options\'s max_time_deviation: %s' % describe_response.table_options.max_time_deviation)
    print ('Secondary indexes:')
    for secondary_index in describe_response.secondary_indexes:
        print ('index name: %s' % secondary_index.index_name)
        print ('primary key names: %s' % str(secondary_index.primary_key_names))
        print ('defined column names: %s' % str(secondary_index.defined_column_names))
    print ('End DescribeTable')

def delete_index(client, index_name):
    print ('Begin DeleteIndex')
    client.delete_secondary_index(table_name, index_name)
    print ('End delete index.')

def delete_table(client):
    print ('Begin DeleteTable')
    client.delete_table(table_name)
    print ('End DeleteTable')

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass

    create_table(client)

    #time.sleep(3) # wait for table ready
    create_index(client)
    describe_table(client)
    delete_index(client, index_name_1)
    describe_table(client)
    delete_index(client, index_name_2)
    delete_table(client)

