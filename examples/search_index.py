# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json

table_name = 'search_index_example_table'
index_name = 'search_index'
nested_index_name = 'nested_search_index'
client = None


def term_query_with_multiple_version_response(table_name, index_name):
    query = TermQuery('k', 'key000')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    print("***** 1.0.0 ~ 5.1.0 version: tuple *****")
    items = search_response.v1_response()
    print(items)

    print("***** 1.0.0 ~ 5.1.0 version: iter *****")
    for item in search_response:
        print(item)

    print("***** 5.2.0 version *****")
    print(search_response.rows)


def match_all_query(table_name, index_name):
    print('********** Begin MatchAllQuery **********')

    # simple queries: match all query and scan to get all data with next token
    query = MatchAllQuery()
    all_rows = []
    next_token = None

    while True:
        search_response = client.search(table_name, index_name,
                                        SearchQuery(query, next_token=next_token, limit=100, get_total_count=True),
                                        columns_to_get=ColumnsToGet(['k', 't', 'g', 'ka', 'la'],
                                                                    ColumnReturnType.SPECIFIED))
        all_rows.extend(search_response.rows)

        if not next_token:  # data all returned
            break

    for row in all_rows:
        print(row)

    print('Total rows: %d' % len(all_rows))

    print('********** End MatchAllQuery **********\n')


def _print_rows(request_id, rows, total_count):
    print('Request ID:%s' % request_id)

    for row in rows:
        print(row)

    print('Rows return: %d' % len(rows))
    print('Total count: %d' % total_count)


def fuzzy_query(table_name, index_name):
    print('********** Begin FuzzyQuery **********')

    query = MatchPhraseQuery('phone', '456')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(['phone'], return_type=ColumnReturnType.SPECIFIED))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End MatchPhraseQuery **********\n')


def term_query(table_name, index_name):
    print('********** Begin TermQuery **********')

    query = TermQuery('k', 'key000')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End TermQuery **********\n')


def collapse_query(table_name, index_name):
    print('********** Begin Collapse **********')

    query = MatchAllQuery()
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True, collapse_field=Collapse('k2')),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End Collapse **********\n')


def range_query(table_name, index_name):
    print('********** Begin RangeQuery **********')

    query = RangeQuery('k', 'key100', 'key500', include_lower=False, include_upper=False)
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End RangeQuery **********\n')


def range_time_query(table_name, index_name):
    print('********** Begin RangeTimeQuery **********')

    query = RangeQuery('time', '2022-05-08', '2022-05-12', include_lower=True, include_upper=True)
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End RangeTimeQuery **********\n')


def prefix_query(table_name, index_name):
    print('********** Begin PrefixQuery **********')

    query = PrefixQuery('k', 'key00')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End PrefixQuery **********\n')


def wildcard_query(table_name, index_name):
    print('********** Begin WildcardQuery **********')

    query = WildcardQuery('k', 'key00*')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End WildcardQuery **********\n')


def terms_query(table_name, index_name):
    print('********** Begin TermsQuery **********')

    query = TermsQuery('k', ['key000', 'key100', 'key888', 'key999', 'key908', 'key1000'])
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End TermsQuery **********\n')


def bool_query(table_name, index_name):
    print('********** Begin BoolQuery **********')

    # k > 'key100' and (l > 110 and l < 200) and not (k = 'key121')
    # and should_queries(k > 'key120' or l < 300, minimum_should_match=2)
    bool_query = BoolQuery(
        must_queries=[
            RangeQuery('k', range_from='key100', include_lower=False),
            BoolQuery(
                must_queries=[
                    RangeQuery('l', range_from=110, include_lower=False),
                    RangeQuery('l', range_to=200, include_upper=False)
                ],
            )
        ],
        must_not_queries=[
            TermQuery('k', 'key121')
        ],
        should_queries=[
            RangeQuery('k', range_from='key120', include_lower=False),
            RangeQuery('l', range_to=300, include_upper=130)
        ],
        minimum_should_match=2
    )

    search_response = client.search(table_name, index_name,
                                    SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.ASC)]),
                                                limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End BoolQuery **********\n')


def geo_distance_query(table_name, index_name):
    print('********** Begin GeoDistanceQuery **********')

    query = GeoDistanceQuery('g', '32.5,116.5', 300000)
    sort = Sort(sorters=[
        GeoDistanceSort('g', ['32.5,116.5', '32.0,116.0'], sort_order=SortOrder.DESC)
    ])
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True, sort=sort),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End GeoDistanceQuery **********\n')


def geo_bounding_box_query(table_name, index_name):
    print('********** Begin GeoBoundingBoxQuery **********')

    query = GeoBoundingBoxQuery('g', '30.9,112.0', '30.2,119.0')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End GeoBoundingBoxQuery **********\n')


def geo_polygon_query(table_name, index_name):
    print('********** Begin GeoPolygonQuery **********')

    query = GeoPolygonQuery('g', ['30.9,112.0', '30.5,115.0', '30.3, 117.0', '30.2,119.0'])
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End GeoPolygonQuery **********\n')


def nested_query(table_name, index_name):
    print('********** Begin NestedQuery **********')

    nested_query = RangeQuery('n.nl', range_from=110, range_to=200, include_lower=True, include_upper=True)
    query = NestedQuery('n', nested_query)
    sort = Sort(
        sorters=[FieldSort('n.nl', sort_order=SortOrder.ASC,
                           nested_filter=NestedFilter('n', RangeQuery('n.nl', range_from=150, range_to=200)))]
    )
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True, sort=sort),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End NestedQuery **********\n')


def prepare_data(rows_count):
    print('Begin prepare data: %d' % rows_count)
    for i in range(rows_count):
        pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
        lj = i / 100
        li = i % 100
        cols = [('k', 'key%03d' % i), ('t', 'this is ' + str(i)), ('k2', '%d' % (i / 10)),
                ('g', '%f,%f' % (30.0 + 0.05 * lj, 114.0 + 0.05 * li)), ('ka', '["a", "b", "%d"]' % i),
                ('la', '[-1, %d]' % i), ('l', i), ('phone', '177712345%d78' % (i % 10)),
                ('b', i % 2 == 0), ('d', 0.1), ('time', '2022-05-%d' % (i % 31 + 1)),
                ('n', json.dumps([{'nk': 'key%03d' % i, 'nl': i, 'nt': 'this is in nested ' + str(i)}]))
                ]

        client.put_row(table_name, Row(pk, cols))

    print('End prepare data.')
    print('Wait for data sync to search index.')
    time.sleep(60)


def prepare_table():
    print('********** Begin CreateTable **********\n')

    table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

    table_options = TableOptions(allow_update=False)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)

    print('********** End CreateTable **********\n')


def prepare_index(index_name, with_nested=False):
    print('********** Begin CreateSearchIndex **********\n')

    field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
    field_a2 = FieldSchema('k2', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
    field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
    field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
    field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
    field_e = FieldSchema('la', FieldType.LONG, index=True, is_array=True, store=True)
    field_f = FieldSchema('l', FieldType.LONG, index=True, store=True)
    field_g = FieldSchema('b', FieldType.BOOLEAN, index=True, store=True)
    field_h = FieldSchema('d', FieldType.DOUBLE, index=True, store=True)
    field_i = FieldSchema('time', FieldType.DATE, index=True, store=True, date_formats=["yyyy-MM-dd"])
    field_j = FieldSchema('phone', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.FUZZY,
                          analyzer_parameter=FuzzyAnalyzerParameter(1, 6))
    field_vl = FieldSchema('vl', FieldType.KEYWORD, index=True, store=True, is_virtual_field=True, source_fields=['l'])

    if with_nested:
        field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
            FieldSchema('nk', FieldType.KEYWORD, index=True, store=True),
            FieldSchema('nl', FieldType.LONG, index=True, store=True)
        ])

    fields = [field_a, field_a2, field_b, field_c, field_d, field_e, field_f, field_g,
              field_h, field_i, field_j, field_vl]

    if with_nested:
        fields.append(field_n)
    index_setting = IndexSetting(routing_fields=['PK1'])
    index_sort = Sort(sorters=[PrimaryKeySort(SortOrder.ASC)]) if not with_nested else None
    index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort,
                                 time_to_live=24 * 3600 * 365)  # default with index sort
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
    delete_search_index(nested_index_name)
    delete_table()

    prepare_table()
    prepare_index(index_name, with_nested=False)
    prepare_index(nested_index_name, with_nested=True)
    prepare_data(100)
    list_search_index()
    describe_search_index()
    update_search_index()
    describe_search_index()

    # perform queries
    term_query_with_multiple_version_response(table_name, index_name)
    match_all_query(table_name, index_name)
    term_query(table_name, index_name)
    range_query(table_name, index_name)
    prefix_query(table_name, index_name)
    terms_query(table_name, index_name)
    bool_query(table_name, index_name)
    wildcard_query(table_name, index_name)
    geo_distance_query(table_name, index_name)
    geo_bounding_box_query(table_name, index_name)
    geo_polygon_query(table_name, index_name)
    nested_query(table_name, nested_index_name)
    range_time_query(table_name, index_name)
    fuzzy_query(table_name, index_name)
    collapse_query(table_name, index_name)

    delete_search_index(index_name)
    delete_search_index(nested_index_name)
    delete_table()
