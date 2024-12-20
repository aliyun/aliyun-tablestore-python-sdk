# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
from tablestore.metadata import *
import time

tags = {"tag1": "t1", "tag2": "t2"}
field1 = {"long_field": 1, "string_field": "string", "bool_field": True, "double_field": 0.3}
field2 = {"binary_field2": b'a'}
key2 = TimeseriesKey("measure2", "datasource2", tags)
key1 = TimeseriesKey("measure1", "datasource1", tags)

def create_timeseries_table(client: OTSClient, table_name: str):
    tableOption = TimeseriesTableOptions(172800)
    metaOption = TimeseriesMetaOptions(None, False)
    tableMeta = TimeseriesTableMeta(table_name, tableOption, metaOption)
    request = CreateTimeseriesTableRequest(tableMeta)
    ret = client.create_timeseries_table(request)
    print(ret)


def get_timeseries_meta(client: OTSClient, table_name: str):
    request = QueryTimeseriesMetaRequest(table_name)
    resp = client.query_timeseries_meta(request)
    print(resp.nextToken)
    print(resp.timeseriesMetas)
    print(resp.totalHits)


def update_timeseries_meta(client: OTSClient, table_name: str):
    attri = {"aaa": "bbb", "ccc": "dddd"}
    meta = TimeseriesMeta(key1, attri)
    req = UpdateTimeseriesMetaRequest(table_name, [meta])
    client.update_timeseries_meta(req)

def delete_timeseries_meta(client: OTSClient, table_name: str):
    req = DeleteTimeseriesMetaRequest(table_name, [key2])
    client.delete_timeseries_meta(req)



def put_timeseries_data(client: OTSClient, table_name: str):
    time1 = time.time()
    row1 = TimeseriesRow(key1, field1, int(time1 * 1000000))
    time2 = time.time()
    row2 = TimeseriesRow(key2, field2, int(time2 * 1000000))
    rows = [row1, row2]

    client.put_timeseries_data(table_name, rows)




if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE)
    table_name = "table_name"
    create_timeseries_table(client, table_name)
    time.sleep(30)
    put_timeseries_data(client, table_name)

    get_timeseries_meta(client, table_name)
    update_timeseries_meta(client, table_name)
    delete_timeseries_meta(client, table_name)

    client.describe_timeseries_table(table_name)



