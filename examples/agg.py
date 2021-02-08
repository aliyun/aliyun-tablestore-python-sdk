# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json

table_name = 'AggExampleTable'
index_name = 'search_index_agg'
client = None

"""
TABLE DATA:

PK1 | PK2  |    k   |    t      |     g     |     ka     |   la    | l |   b   |  d  |  n.t     |  nk    | nl |
---------------------------------------------------------------------------------------------------------------
 0  | pk_0 | key000 | this is 0 | 30,114    | ["a", "0"] | [-1, 0] | 0 | True  | 0.1 | nested 0 | key000 | 0  | 
 1  | pk_1 | key001 | this is 1 | 30,114.05 | ["a", "1"] | [-1, 1] | 1 | False | 0.1 | nested 1 | key001 | 1  | 
 2  | pk_2 | key002 | this is 2 | 30,114.10 | ["a", "2"] | [-1, 2] | 2 | True  | 0.1 | nested 2 | key002 | 2  | 
 3  | pk_3 | key003 | this is 3 | 30,114.15 | ["a", "3"] | [-1, 3] | 3 | False | 0.1 | nested 3 | key003 | 3  | 
 4  | pk_4 | key004 | this is 4 | 30,114.20 | ["a", "4"] | [-1, 4] | 4 | True  | 0.1 | nested 4 | key004 | 4  | 
 5  | pk_5 | key005 | this is 5 | 30,114.25 | ["a", "5"] | [-1, 5] |   | False | 0.1 | nested 5 | key005 | 5  | 
 6  | pk_6 | key006 | this is 6 | 30,114.30 | ["a", "6"] | [-1, 6] | 6 | True  | 0.1 | nested 6 | key006 | 6  | 
 7  | pk_7 | key007 | this is 7 | 30,114.35 | ["a", "7"] | [-1, 7] | 7 | False | 0.1 | nested 7 | key007 | 7  | 
 8  | pk_8 | key008 | this is 8 | 30,114.40 | ["a", "8"] | [-1, 8] | 8 | True  | 0.1 | nested 8 | key008 | 8  | 
 9  | pk_9 | key009 | this is 9 | 30,114.45 | ["a", "9"] | [-1, 9] | 9 | False | 0.1 | nested 9 | key009 | 9  | 
"""

"""

Sample 1: 

SQL: SELECT MAX(l) as max FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: max: 9.0 

"""
def max_agg(table_name, index_name):
    print('**** Begin Sample 1 ****\n')

    query = TermQuery('d', 0.1)
    agg = Max('l', name = 'max')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=0, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 1.1: 

SQL: SELECT MAX(ifnull(l, 100)) as max FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: max: 100

"""
def max_agg(table_name, index_name):
    print('**** Begin Sample 1.1 ****\n')

    query = TermQuery('d', 0.1)
    agg = Max('l', missing_value = 100, name = 'max')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=0, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 2:

SQL: SELECT MIN(l) as min FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: min: 0

"""
def min_agg(table_name, index_name):
    print('**** Begin Sample 2 ****\n')

    query = TermQuery('d', 0.1)
    agg = Min('l', name = 'min')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=0, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))
    

"""

Sample 3:

SQL: SELECT AVG(l) as avg FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: avg: 4.0

"""
def avg_agg(table_name, index_name):
    print('**** Begin Sample 3 ****\n')

    query = TermQuery('d', 0.1)
    agg = Avg('l', name = 'avg')

    search_response = client.search(table_name, index_name,
                                     SearchQuery(query, next_token = None, limit=2, aggs=[agg]),
                                     columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 4:

SQL: SELECT SUM(l) as sum FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: sum: 40

"""    
def sum_agg(table_name, index_name):
    print('**** Begin Sample 4 ****\n')

    query = TermQuery('d', 0.1)
    agg = Sum('l', name = 'sum')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=2, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 5:

SQL: SELECT COUNT(l) as count FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: count: 9

"""
def count_agg(table_name, index_name):
    print('**** Begin Sample 5 ****\n')

    query = TermQuery('d', 0.1)
    agg = Count('l', name = 'count')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=2, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 6:

SQL: SELECT DISTINCT COUNT(l) as dcount FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: dcount: 9

"""        
def distinct_count_agg(table_name, index_name):
    print('**** Begin Sample 6 ****\n')

    query = TermQuery('d', 0.1)
    agg = DistinctCount('l', name = 'dcount')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=2, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 7:

SQL: SELECT TOP 3 FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result: 
([('PK1', 0), ('PK2', 'pk_0')])
([('PK1', 1), ('PK2', 'pk_1')])
([('PK1', 2), ('PK2', 'pk_2')])

"""    
def top_rows_agg(table_name, index_name):
    print('**** Begin Sample 7 ****\n')

    query = TermQuery('d', 0.1)
    agg = TopRows(limit = 3, sort = Sort([PrimaryKeySort()]))

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=0, aggs=[agg]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.NONE))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

"""

Sample 8:

SQL: SELECT SUM(l) as s1, SUM(n.nl) as s2, COUNT(l) as c1 FROM AggExampleTable.search_index_agg WHERE d = 0.1 

Result:
s1 : 40
s2 : 45
c1 : 9

"""
def multiple_agg(table_name, index_name):
    print('**** Begin Sample 8 ****\n')

    query = TermQuery('d', 0.1)
    agg1 = Sum('l', name = 's1')
    agg2 = Sum('n.nl', name = 's2')
    agg3 = Count('l', name = 'c1')

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit = 0, aggs = [agg1, agg2, agg3]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for agg_result in search_response.agg_results:
        print('{\n"name":"%s",\n"value":%s\n}\n' % (agg_result.name, str(agg_result.value)))

def prepare_data(rows_count):
    print ('Begin prepare data: %d' % rows_count)
    for i in range(rows_count):
        pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
        lj = i / 100
        li = i % 100
        cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)),
            ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "%d"]' % i),
            ('la', '[-1, %d]' % i), ('l', i),
            ('b', i % 2 == 0), ('d', 0.1),
            ('n', json.dumps([{'nk':'key%03d' % i, 'nl':i, 'nt':'nested ' + str(i)}]))]

        if i == 5:
            cols.remove(('l', 5))
        client.put_row(table_name, Row(pk, cols))

    print ('End prepare data.')
    print ('Wait for data sync to search index.')
    time.sleep(10)

def prepare_table():
    table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)

def prepare_index(index_name):
    field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
    field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
    field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
    field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
    field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
    field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
    field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
    field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)

    field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
        FieldSchema('nk', FieldType.KEYWORD, index=True, store=True),
        FieldSchema('nl', FieldType.LONG, index=True, store=True),
        FieldSchema('nt', FieldType.TEXT, index=True, store=True),
    ])

    fields = [field_a, field_b, field_c, field_d, field_e, field_f, field_g, field_h]
    fields.append(field_n)
    index_setting = IndexSetting(routing_fields=['PK1'])
    index_sort = None
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
    prepare_index(index_name)
    prepare_data(10)

    time.sleep(30)

    # perform queries
    max_agg(table_name, index_name)
    min_agg(table_name, index_name)
    sum_agg(table_name, index_name)
    avg_agg(table_name, index_name)
    count_agg(table_name, index_name)
    distinct_count_agg(table_name, index_name)
    top_rows_agg(table_name, index_name) 
    multiple_agg(table_name, index_name)

    delete_search_index(index_name)
    delete_table()
