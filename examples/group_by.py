# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json

table_name = 'group_by_example_table'
index_name = 'search_index_group_by'
client = None

def group_by_field(table_name, index_name):
    print('**** Begin Sample 1 ****\n')

    query = TermQuery('d', 0.1)
    group_by = GroupByField('l', size = 10)

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=20, group_bys = [group_by]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        for item in group_by.items:
            print("key:%s, count:%d" % (item.key, item.row_count))

def group_by_field_with_group_sort(table_name, index_name):
    print('**** Begin Sample 2 ****\n')

    group_by = GroupByField('l', size = 2, group_by_sort = [RowCountSort(sort_order=SortOrder.ASC)])
    search_response = client.search(table_name, index_name, 
                                    SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
                                    ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        for item in group_by.items:
            print("key:%s, count:%d" % (item.key, item.row_count))

def group_by_field_with_sub_agg(table_name, index_name):
    print('**** Begin Sample 3 ****\n')

    sort = RowCountSort(sort_order = SortOrder.ASC)
    sub_agg = [TopRows(limit=3,sort=Sort([PrimaryKeySort(sort_order=SortOrder.DESC)]), name = 't1')]

    group_by = GroupByField('l', size = 2, group_by_sort = [sort], sub_aggs = sub_agg)
    search_response = client.search(table_name, index_name, 
                                    SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
                                    ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        for item in group_by.items:
            print("\tkey:%s, count:%d" % (item.key, item.row_count))
            for sub_agg in item.sub_aggs:
                print("\t\tname:%s:" % sub_agg.name)
                for entry in sub_agg.value:
                    print("\t\t\tvalue:%s" % str(entry))

def group_by_field_with_sub_group(table_name, index_name):
    print('**** Begin Sample 4 ****\n')

    sort = RowCountSort(sort_order = SortOrder.ASC)
    sub_group = GroupByField('b', size = 10, group_by_sort = [sort])

    group_by = GroupByField('l', size = 10, group_by_sort = [sort], sub_group_bys = [sub_group])
    search_response = client.search(table_name, index_name, 
                                    SearchQuery(TermQuery('d', 0.1), limit=100, get_total_count=True, group_bys = [group_by]), 
                                    ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        for item in group_by.items:
            print("\tkey:%s, count:%d" % (item.key, item.row_count))
            for sub_group in item.sub_group_bys:
                print("\t\tname:%s:" % sub_group.name)
                for sub_item in sub_group.items:
                    print("\t\t\tkey:%s, count:%s" % (str(sub_item.key), str(sub_item.row_count)))

def group_by_range(table_name, index_name):
    print('**** Begin Sample 5 ****\n')

    query = TermQuery('d', 0.1)
    group_by = GroupByRange(field_name = 'la', ranges = [(0, 10),(10, 20)])

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=0, group_bys = [group_by]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        for item in group_by.items:
            print("range:%.1f~%.1f, count:%d" % (item.range_from, item.range_to, item.row_count))


def group_by_filter(table_name, index_name):
    print('**** Begin Sample 6 ****\n')

    query = TermQuery('d', 0.1)
    filter1 = TermQuery('l', 1)
    filter2 = TermQuery('ka', "a")
    filters = [filter1, filter2]
    group_by = GroupByFilter(filters)

    search_response = client.search(
        table_name, index_name,
        SearchQuery(query, next_token = None, limit=2, group_bys = [group_by]),
        columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        i = 0
        for item in group_by.items:
            print("filter:%s=%s, count:%d" % (str(filters[i].field_name), str(filters[i].column_value), item.row_count))
            i=i+1

def group_by_geo_distance(table_name, index_name):
    print('**** Begin Sample 7 ****\n')

    query = TermQuery('d', 0.1)
    group_by = GroupByGeoDistance(field_name = 'g', origin=GeoPoint(31, 116), ranges = [(0, 100000), (100000,1000000)])

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, next_token = None, limit=2, group_bys = [group_by]),
                                    columns_to_get = ColumnsToGet(return_type = ColumnReturnType.ALL_FROM_INDEX))

    for group_by in search_response.group_by_results:
        print("name:%s" % group_by.name)
        print("groups:")
        for item in group_by.items:
            print("range:%.1f~%.1f, count:%d" % (item.range_from, item.range_to, item.row_count))


def prepare_data(rows_count):
    print ('Begin prepare data: %d' % rows_count)
    for i in range(rows_count):
        pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
        lj = i / 100
        li = i % 100
        cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)),
            ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "b", "%d"]' % i),
            ('la', '[-1, %d]' % i), ('l', i%3),
            ('b', i % 2 == 0), ('d', 0.1),
            ('n', json.dumps([{'nk':'key%03d' % i, 'nl':i, 'nt':'this is in nested ' + str(i)}]))]

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
    prepare_data(100)

    time.sleep(30)

    # perform group by
    group_by_field(table_name, index_name)
    group_by_field_with_group_sort(table_name, index_name)
    group_by_field_with_sub_agg(table_name, index_name)
    group_by_field_with_sub_group(table_name, index_name)
    group_by_range(table_name, index_name)
    group_by_filter(table_name, index_name)
    group_by_geo_distance(table_name, index_name)

    delete_search_index(index_name)
    delete_table()
