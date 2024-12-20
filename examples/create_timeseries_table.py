# -*- coding: utf8 -*-

from example_config import *
from tablestore import *
from tablestore.metadata import *
import time


def create_timeseries_table(client: OTSClient):
    tableOption = TimeseriesTableOptions(172800)
    metaOption = TimeseriesMetaOptions(None, False)
    tableMeta = TimeseriesTableMeta("table_name", tableOption, metaOption)
    analytical_store = TimeseriesAnalyticalStore("as", 2592000, SyncType.SYNC_TYPE_FULL)
    lastPointIndex = LastpointIndexMeta("last1")
    request = CreateTimeseriesTableRequest(tableMeta, [analytical_store], [lastPointIndex])
    ret = client.create_timeseries_table(request)
    print(ret)


def create_timeseries_table_with_user_define_key(client: OTSClient):
    tableOption = TimeseriesTableOptions(172800)
    metaOption = TimeseriesMetaOptions(None, False)
    timeseries_keys = ["a", "b"]
    field_primary_keys = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    tableMeta = TimeseriesTableMeta("table_name", tableOption, metaOption, timeseries_keys, field_primary_keys)
    analytical_store = TimeseriesAnalyticalStore("as", 2592000, SyncType.SYNC_TYPE_FULL)
    lastPointIndex = LastpointIndexMeta("last1")
    request = CreateTimeseriesTableRequest(tableMeta, [analytical_store], True, [lastPointIndex])
    ret = client.create_timeseries_table(request)
    print(ret)


def delete_timeseries_table(client: OTSClient):
    client.delete_timeseries_table("table_name")


def describe_timeseries_table(client: OTSClient):
    client.describe_timeseries_table("table_name")


if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ACCESS_KEY_ID, OTS_ACCESS_KEY_SECRET, OTS_INSTANCE)
    create_timeseries_table(client)
    time.sleep(30)
    describe_timeseries_table(client)
    delete_timeseries_table(client)
