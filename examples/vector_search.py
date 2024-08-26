# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json

table_name = 'vector_search_example_table'
index_name = 'search_index'
client = None


def _print_rows(request_id, rows, total_count):
    print('Request ID:%s' % request_id)

    for row in rows:
        print(row)

    print('Rows return: %d' % len(rows))
    print('Total count: %d' % total_count)


def knn_vector_query(table_name, index_name):
    print('********** Begin KNN Vector Query **********')

    query = KnnVectorQuery(field_name='vq', top_k=10, float32_query_vector=[5, -5, 10, -10], filter=MatchAllQuery())
    sort = Sort(sorters=[ScoreSort(sort_order=SortOrder.DESC)])
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=10, get_total_count=False, sort=sort),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX))
    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End KNN Vector Query **********')


def prepare_data(rows_count):
    print('Begin prepare data: %d' % rows_count)
    for i in range(rows_count):
        pk = [('PK1', i)]

        cols = [('k', 'key%03d' % i),
                ('l', i),
                ('vq', '[%d, %d, %d, %d]' % (i + 5, i - 5, i + 10, i - 10))]

        client.put_row(table_name, Row(pk, cols))

    print('End prepare data.')
    print('Wait for data sync to search index.')
    time.sleep(60)


def prepare_table():
    print('********** Begin CreateTable **********\n')

    table_meta = TableMeta(table_name, [('PK1', 'INTEGER')])

    table_options = TableOptions(allow_update=False)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)

    print('********** End CreateTable **********\n')


def prepare_index(index_name):
    print('********** Begin CreateSearchIndex **********\n')

    field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
    field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
    field_vq = FieldSchema("vq", FieldType.VECTOR, vector_options=VectorOptions(
        data_type=VectorDataType.VD_FLOAT_32, metric_type=VectorMetricType.VM_COSINE, dimension=4))

    fields = [field_a, field_f, field_vq]

    index_setting = IndexSetting(routing_fields=['PK1'])
    index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)])
    index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort)  # default with index sort
    client.create_search_index(table_name, index_name, index_meta)

    print('********** End CreateSearchIndex **********\n')


def update_search_index():
    print('********** Begin ListSearchIndex **********\n')

    index_meta = SearchIndexMeta(fields=None, time_to_live=24 * 3600 * 180)
    client.update_search_index(table_name, index_name, index_meta)
    print('End update search index')


def list_search_index():
    for table, index_name in client.list_search_index(table_name):
        print('%s, %s' % (table, index_name))

    print('********** End ListSearchIndex **********\n')


def describe_search_index():
    print('********** Begin DescribeSearchIndex **********\n')

    index_meta, sync_stat = client.describe_search_index(table_name, index_name)
    print('sync stat: %s, %d' % (str(sync_stat.sync_phase), sync_stat.current_sync_timestamp))
    print('index name: %s' % index_name)
    print('ttl: %ds' % (index_meta.time_to_live))
    print('index fields:')
    print('name \t type \t\t indexed \t stored \t is_array \t allow_sort \t ')
    for field in index_meta.fields:
        print('%s\t%s\t%s\t\t%s\t\t%s\t%s' % (field.field_name, str(field.field_type),
                                              str(field.index), str(field.store),
                                              str(field.is_array), str(field.enable_sort_and_agg)))

    print('********** End DescribeSearchIndex **********\n')


def delete_table():
    print('********** Begin DeleteTable **********\n')

    try:
        client.delete_table(table_name)
    except:
        pass

    print('********** End DeleteTable **********\n')


def delete_search_index(index_name):
    print('********** Begin DeleteSearchIndex **********\n')

    try:
        client.delete_search_index(table_name, index_name)
    except:
        pass
    print('********** End DeleteSearchIndex **********\n')


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE)
    delete_search_index(index_name)
    delete_table()

    prepare_table()
    prepare_index(index_name)
    prepare_data(100)
    list_search_index()
    describe_search_index()
    update_search_index()
    describe_search_index()

    # perform queries
    knn_vector_query(table_name, index_name)

    delete_search_index(index_name)
    delete_table()
