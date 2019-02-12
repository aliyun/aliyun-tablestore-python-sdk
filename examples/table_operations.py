# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time

table_name = 'OTSTableOperationsSimpleExample'

def create_table(client):
    print ('Begin CreateTable')
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions(-1, 2)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_option, reserved_throughput)
    print ('Table has been created.')

def list_table(client):
    print ('Begin ListTable')
    tables = client.list_table()
    print ('All the tables you have created:')
    for table in tables:
        print (table)
    print ('End ListTable')

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
    print ('End DescribeTable')

def update_table(client):
    print ('Begin UpdateTable')
    table_option = TableOptions(100001, None, None)
    update_response = client.update_table(table_name, table_option, None)
    print ('Reserved read throughput: %s' % update_response.reserved_throughput_details.capacity_unit.read)
    print ('Reserved write throughput: %s' % update_response.reserved_throughput_details.capacity_unit.write)
    print ('Last increase throughput time: %s' % update_response.reserved_throughput_details.last_increase_time)
    print ('Last decrease throughput time: %s' % update_response.reserved_throughput_details.last_decrease_time)
    print ('table options\'s time to live: %s' % update_response.table_options.time_to_live)
    print ('table options\'s max version: %s' % update_response.table_options.max_version)
    print ('table options\'s max_time_deviation: %s' % update_response.table_options.max_time_deviation)
    print ('End UpdateTable')

def delete_table(client):
    print ('Begin DeleteTable')
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass

    create_table(client)

    time.sleep(3) # wait for table ready

    list_table(client)
    describe_table(client)
    update_table(client)
    delete_table(client)

