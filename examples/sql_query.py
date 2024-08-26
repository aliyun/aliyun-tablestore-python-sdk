from example_config import *
from tablestore import *
import time

table_name = 'OTSSqlQuerySimpleExample'


def exe_sql_query(client, query):
    row_list, table_comsume_list, search_comsume_list = client.exe_sql_query(query)
    if len(table_comsume_list) > 0:
        for table_comsume_unit in table_comsume_list:
            print('Read succeed, table %s consume %s read cu.' % (table_comsume_unit[0], table_comsume_unit[1].read))
    if len(search_comsume_list) > 0:
        for search_comsume_unit in search_comsume_list:
            print('Read succeed, table %s consume %s read cu.' % (search_comsume_unit[0], search_comsume_unit[1].read))
    for row in row_list:
        print(row.attribute_columns)


create_queries = [
    'show tables',
    'create table %s (uid VARCHAR(1024), pid BIGINT(20),name MEDIUMTEXT, age BIGINT(20), grade DOUBLE, PRIMARY KEY(uid,pid));' % table_name,
    'desc %s' % table_name,
]

select_and_delete_queries = [
    'select * from %s' % table_name,
    'select * from %s where pid >3' % table_name,
    'select grade from %s where uid = "7"' % table_name,
    'drop mapping table %s' % table_name
]


def create_table(client):
    table_meta = TableMeta(table_name, [('uid', 'STRING'), ('pid', 'INTEGER')])
    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
    time.sleep(5)


def batch_write_row(client):
    # batch put 10 rows and update 10 rows on exist table, delete 10 rows on a not-exist table.
    put_row_items = []
    for i in range(0, 10):
        primary_key = [('uid', str(i)), ('pid', i)]
        attribute_columns = [('name', 'somebody' + str(i)), ('age', i), ('grade', i + 0.2)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        item = PutRowItem(row, condition)
        put_row_items.append(item)

    request = BatchWriteRowRequest()
    request.add(TableInBatchWriteRowItem(table_name, put_row_items))
    result = client.batch_write_row(request)

    print('Result status: %s' % (result.is_all_succeed()))
    print('check first table\'s put results:')
    succ, fail = result.get_put()
    for item in succ:
        print('Put succeed, consume %s write cu.' % item.consumed.write)
    for item in fail:
        print('Put failed, error code: %s, error message: %s' % (item.error_code, item.error_message))


def drop_table(client):
    client.delete_table(table_name)


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE)
    create_table(client)
    for query in create_queries:
        exe_sql_query(client, query)
    batch_write_row(client)
    for query in select_and_delete_queries:
        exe_sql_query(client, query)
    drop_table(client)
