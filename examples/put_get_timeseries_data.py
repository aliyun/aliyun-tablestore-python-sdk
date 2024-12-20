# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
from tablestore.metadata import *
import time


def put_timeseries_data(client: OTSClient, table_name: str):
    tags = {"tag1": "t1", "tag2": "t2"}
    field1 = {"long_field": 1, "string_field": "string", "bool_field": True, "double_field": 0.3}
    field2 = {"binary_field2": bytearray(b'a')}
    key2 = TimeseriesKey("measure2", "datasource2", tags)
    key1 = TimeseriesKey("measure1", "datasource1", tags)
    time1 = time.time()
    row1 = TimeseriesRow(key1, field1, int(time1 * 1000000))
    time2 = time.time()
    row2 = TimeseriesRow(key2, field2, int(time2 * 1000000))
    rows = [row1, row2]

    client.put_timeseries_data(table_name, rows)


def get_timeseries_data(client: OTSClient, table_name: str):
    request = GetTimeseriesDataRequest(table_name)
    tags = {"tag1": "t1", "tag2": "t2"}
    key1 = TimeseriesKey("measure1", "datasource1", tags)
    request.timeseriesKey = key1
    request.endTimeInUs = int(time.time() * 1000000)
    resp = client.get_timeseries_data(request)
    print(resp.rows)
    print(resp.nextToken)


def create_timeseries_data(client: OTSClient, table_name: str):
    table_meta = TimeseriesTableMeta(table_name, TimeseriesTableOptions(172800), TimeseriesMetaOptions(None, False))
    request = CreateTimeseriesTableRequest(table_meta)
    client.create_timeseries_table(request)


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE)
    table_name = "table_name"
    create_timeseries_data(client, table_name)
    put_timeseries_data(client, table_name)
    get_timeseries_data(client, table_name)