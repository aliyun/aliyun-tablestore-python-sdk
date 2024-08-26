# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
import time
import json

table_name = 'example_full_text_search_table'
index_name = 'example_full_text_search_index'
client = None


def term_query_with_multiple_version_response():
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


def _print_rows(request_id, rows, total_count):
    print('Request ID:%s' % request_id)

    for row in rows:
        print(row)

    print('Rows return: %d' % len(rows))
    print('Total count: %d' % total_count)


def match_query():
    print('********** Begin MatchQuery **********')

    query = MatchQuery('title', 'hangzhou')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL)
                                    )

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** Begin MatchQuery **********')


def match_phrase_query():
    print('********** Begin MatchPhraseQuery **********')

    query = MatchPhraseQuery('title', 'hangzhou')
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End MatchPhraseQuery **********\n')


def bool_query():
    print('********** Begin BoolQuery **********')

    # k > 'key100' and (l > 110 and l < 200) and not (k = 'key121')
    # and should_queries(k > 'key120' or l < 300, minimum_should_match=2)
    bool_query = BoolQuery(
        must_queries=[
            RangeQuery('k', range_from='key100', include_lower=False),
            BoolQuery(
                must_queries=[
                    MatchQuery('title', 'hangzhou')
                ],
            )
        ],
        must_not_queries=[
            TermQuery('k', 'key121')
        ],
        should_queries=[
            RangeQuery('k', range_from='key120', include_lower=False),
            MatchQuery('title', 'xihu')
        ],
        minimum_should_match=2
    )

    search_response = client.search(table_name, index_name,
                                    SearchQuery(bool_query, sort=Sort(sorters=[FieldSort('l', SortOrder.ASC)]),
                                                limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End BoolQuery **********\n')


def nested_query():
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


def function_score_query():
    print('********** Begin FunctionScoreQuery **********')

    search_query = MatchQuery('title', "xihu")
    score_query = FunctionScoreQuery(search_query,
                                     FieldValueFactor('l')
                                     )

    search_response = client.search(table_name, index_name,
                                    SearchQuery(score_query, limit=100, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL))

    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('********** End FunctionScoreQuery **********\n')


def highlight_query():
    print('********** Begin HighlightQuery **********')

    query = MatchQuery('title', 'xihu')
    highlight_parameter = HighlightParameter("title", 1, 18, '<b>', '</b>', HighlightFragmentOrder.TEXT_SEQUENCE)
    highlight_clause = Highlight([highlight_parameter], HighlightEncoder.PLAIN_MODE)
    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=2, get_total_count=True,
                                                highlight=highlight_clause),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)
                                    )

    print('----- Print Rows:')
    print('search rows count:%d' % len(search_response.rows))
    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('----- Print Highlight Result:')
    search_hits = search_response.search_hits
    print('search hit count:%d' % len(search_hits))

    for search_hit in search_hits:
        print('\t score is %.6f' % search_hit.score)
        for highlight_field in search_hit.highlight_result.highlight_fields:
            print('\t\t highlight:%s:%s' % (highlight_field.field_name, highlight_field.field_fragments))

    print('********** End HighlightQuery **********')


def highlight_query_for_nested():
    print('********** Begin HighlightQueryForNested **********')

    sort = Sort(
        sorters=[FieldSort('n.nl', sort_order=SortOrder.ASC)]
    )

    highlight_parameter = HighlightParameter("n.nt", 1, 18, '<b>', '</b>', HighlightFragmentOrder.TEXT_SEQUENCE)
    highlight_clause = Highlight([highlight_parameter], HighlightEncoder.PLAIN_MODE)

    inner_hits_parameter = InnerHits(None, 0, 10, highlight_clause)
    query = NestedQuery('n',  MatchQuery('n.nt', 'nested'), ScoreMode.AVG, inner_hits_parameter)

    search_response = client.search(table_name, index_name,
                                    SearchQuery(query, limit=2, get_total_count=True),
                                    ColumnsToGet(return_type=ColumnReturnType.ALL_FROM_INDEX)
                                    )

    print('----- Print Rows:')
    print('search rows count:%d' % len(search_response.rows))
    _print_rows(search_response.request_id, search_response.rows, search_response.total_count)

    print('----- Print Highlight Result:')
    search_hits = search_response.search_hits
    print('search hit count:%d' % len(search_hits))

    _print_search_hit(search_hits)

    print('********** End HighlightQuery **********')


def _print_search_hit(hits):
    for search_hit in hits:
        print('\t score is %.6f' % search_hit.score)
        for highlight_field in search_hit.highlight_result.highlight_fields:
            print('\t\t highlight:%s:%s' % (highlight_field.field_name, highlight_field.field_fragments))
        for inner_result in search_hit.search_inner_hits:
            print('\t\t path:%s' % (inner_result.path))
            _print_search_hit(inner_result.search_hits)


def prepare_data(rows_count):
    print('Begin prepare data: %d' % rows_count)
    for i in range(rows_count):
        pk = [('PK1', i), ('PK2', 'pk_' + str(i % 10))]
        cols = [('k', 'key%03d' % i),
                ('l', i),
                ('title', 'zhongguo zhejiang hangzhou xihu %s road' % str(1900 + i % 100)),
                ('n', json.dumps([{'nl': i, 'nt': 'this is in nested ' + str(i)}]))]

        client.put_row(table_name, Row(pk, cols))

    print('End prepare data.')
    print('Wait for data sync to search index.')
    time.sleep(5)


def prepare_table():
    table_meta = TableMeta(table_name, [('PK1', 'INTEGER'), ('PK2', 'STRING')])

    table_options = TableOptions(allow_update=False)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)


def prepare_index():
    field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True)
    field_b = FieldSchema('l', FieldType.LONG, index=True, enable_sort_and_agg=True)
    field_title = FieldSchema('title', FieldType.TEXT, index=True, analyzer=AnalyzerType.SINGLEWORD,
                              enable_highlighting=True)
    field_n = FieldSchema('n', FieldType.NESTED, sub_field_schemas=[
        FieldSchema('nl', FieldType.LONG, index=True, enable_sort_and_agg=True),
        FieldSchema('nt', FieldType.TEXT, index=True, enable_highlighting=True),
    ])

    fields = [field_a, field_b, field_title, field_n]
    index_setting = IndexSetting(routing_fields=['PK1'])
    index_sort = None
    index_meta = SearchIndexMeta(fields, index_setting=index_setting, index_sort=index_sort,
                                 time_to_live=24 * 3600 * 365)  # default with index sort
    client.create_search_index(table_name, index_name, index_meta)

    print('Wait for create index.')
    wait_for_search_index_ready()


def wait_for_search_index_ready():
    max_wait_time = 400
    interval_time = 20

    while max_wait_time > 0:
        index_meta, sync_stat = client.describe_search_index(table_name, index_name)

        if sync_stat.sync_phase == SyncPhase.INCR:
            delta_time = time.time() - sync_stat.current_sync_timestamp / 1000 / 1000 / 1000
            if delta_time < 20:
                print('Search Index Ready!')
                return
        time.sleep(interval_time)
        max_wait_time = max_wait_time - interval_time


def update_search_index():
    print('********** Begin UpdateSearchIndex **********\n')

    index_meta = SearchIndexMeta(fields=None, time_to_live=24 * 3600 * 180)
    client.update_search_index(table_name, index_name, index_meta)
    print('********** End UpdateSearchIndex **********\n')


def list_search_index():
    print('********** Begin ListSearchIndex **********\n')

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


def delete_search_index():
    print('********** Begin DeleteSearchIndex **********\n')

    try:
        client.delete_search_index(table_name, index_name)
    except:
        pass
    print('********** End DeleteSearchIndex **********\n')


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE)
    delete_search_index()
    delete_table()

    prepare_table()
    prepare_index()
    prepare_data(10)
    list_search_index()
    describe_search_index()
    update_search_index()
    describe_search_index()

    # perform queries
    match_query()
    match_phrase_query()
    bool_query()
    nested_query()
    function_score_query()

    highlight_query()

    highlight_query_for_nested()

    delete_search_index()
    delete_table()
