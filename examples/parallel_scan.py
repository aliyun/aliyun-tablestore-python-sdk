# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json
import threadpool
import threading

table_name = 'ParallelScanExampleTable'
index_name = 'search_index'
client = None

def fetch_rows_per_thread(query, session_id, current_thread_id, max_thread_num):
    token = None

    while True:
        try:
            scan_query = ScanQuery(query, limit = 20, next_token = token, current_parallel_id = current_thread_id, 
                                   max_parallel = max_thread_num, alive_time = 30)
    
            response = client.parallel_scan(
                table_name, index_name, scan_query, session_id, 
                columns_to_get = ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))

            for row in response.rows:
                print("%s:%s" % (threading.currentThread().name, str(row)))

            if len(response.next_token) == 0:
                break
            else:
                token = response.next_token
        except OTSServiceError as e:
            print (e)
        except OTSClientError as e:
            print (e)

def parallel_scan(table_name, index_name):
    response = client.compute_splits(table_name, index_name)

    query = TermQuery('d', 0.1)

    params = []
    for i in range(response.splits_size):
        params.append((([query, response.session_id, i, response.splits_size], None)))

    pool = threadpool.ThreadPool(response.splits_size)
    requests = threadpool.makeRequests(fetch_rows_per_thread, params)  
    [pool.putRequest(req) for req in requests]  
    pool.wait()     


def prepare_data(rows_count):
    print ('Begin prepare data: %d' % rows_count)
    for i in range(rows_count):
        pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
        lj = i / 100
        li = i % 100
        cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)),
            ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "b", "%d"]' % i),
            ('la', '[-1, %d]' % i), ('l', i),
            ('b', i % 2 == 0), ('d', 0.1),
            ('n', json.dumps([{'nk':'key%03d' % i, 'nl':i, 'nt':'this is in nested ' + str(i)}]))]

        client.put_row(table_name, Row(pk, cols))

    print ('End prepare data.')
    print ('Wait for data sync to search index.')
    time.sleep(30)

def prepare_table():
    table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
    time.sleep(10)
    
def prepare_index(index_name, with_nested=False):
    field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
    field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
    field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
    field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
    field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
    field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
    field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
    field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)
    if with_nested:
        field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
            FieldSchema('nk', FieldType.KEYWORD, index=True, store=True),
            FieldSchema('nl', FieldType.LONG, index=True, store=True),
            FieldSchema('nt', FieldType.TEXT, index=True, store=True),
        ])

    fields = [field_a, field_b, field_c, field_d, field_e, field_f, field_g, field_h]
    if with_nested:
        fields.append(field_n)
    index_setting = IndexSetting(routing_fields=['PK1'])
    index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)]) if not with_nested else None
    index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort) # default with index sort
    client.create_search_index(table_name, index_name, index_meta)

def delete_table():
    try:
        client.delete_table(table_name)
    except:
        pass

def delete_search_index(index_name):
    try:
        client.delete_search_index(table_name, index_name)
    except:
        pass

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    delete_search_index(index_name)

    delete_table()

    prepare_table()
    prepare_index(index_name, with_nested=False)
    prepare_data(100)

    # perform parallel scan

    parallel_scan(table_name, index_name)

    delete_search_index(index_name)
    delete_table()
