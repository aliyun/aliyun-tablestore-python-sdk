# -*- coding: utf8 -*-
import unittest

import time
from tablestore import timeseries_condition
from tests.lib.api_test_base import APITestBase
from tablestore import metadata


class TimeseriesMetaConditionTest(APITestBase):
    """TimeseriesMetaConditionTest"""



    def test_timeseries_meta_condition_api(self):
        client = self.client_test
        prefix = "python_sdk_meta_test"
        table_name = prefix + str(int(time.time()))

        try:
            # 清理环境
            b = client.list_timeseries_table()
            for item in b:
                if item.timeseries_table_name.startswith(prefix):
                    client.delete_timeseries_table(item.timeseries_table_name)
        except Exception as e:
            print(e)

        meta = metadata.TimeseriesTableMeta(table_name, metadata.TimeseriesTableOptions(172800))
        request = metadata.CreateTimeseriesTableRequest(meta)
        client.create_timeseries_table(request)
        time.sleep(25)
        # puttimeseriesdata
        tags = {"tag1": "t1", "tag2": "t2"}
        field1 = {"long_field": 1, "string_field": "string", "bool_field": True, "double_field": 0.3}
        field2 = {"binary_field2": bytearray(b'a')}
        key2 = metadata.TimeseriesKey("measure2", "datasource2", tags)
        key1 = metadata.TimeseriesKey("measure1", "datasource1", tags)
        time1 = time.time()
        row1 = metadata.TimeseriesRow(key1, field1, int(time1 * 1000000))
        time2 = time.time()
        row2 = metadata.TimeseriesRow(key2, field2, int(time2 * 1000000))
        rows = [row1, row2]
        client.put_timeseries_data(table_name, rows)

        # update meta
        attri = {"aaa": "bbb", "ccc": "dddd"}
        meta = metadata.TimeseriesMeta(key1, attri)
        req = metadata.UpdateTimeseriesMetaRequest(table_name, [meta])
        client.update_timeseries_meta(req)

        time.sleep(90)

        # measurement condition
        cond = timeseries_condition.MeasurementMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL, key1.measurement_name)
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(res.timeseriesMetas[0].timeseries_key.measurement_name, key1.measurement_name)

        # datasource condition
        cond = timeseries_condition.DataSourceMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                                  key1.measurement_name)
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 0)

        cond = timeseries_condition.DataSourceMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                                 key2.data_source)
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(res.timeseriesMetas[0].timeseries_key.measurement_name, key2.measurement_name)

        # tag condition
        cond = timeseries_condition.TagMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL, "tag1", "f")
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 0)

        cond = timeseries_condition.TagMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                          "tag1", "t1")
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 2)

        cond = timeseries_condition.TagMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_PREFIX,
                                                          "tag1", "t")
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 2)

        # updatetime condition
        time_now = int(time.time() * 1000000)
        cond = timeseries_condition.UpdateTimeMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_GREATER_THAN,
                                                          time_now)
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 0)

        cond = timeseries_condition.UpdateTimeMetaQueryCondition(
            timeseries_condition.MetaQuerySingleOperator.OP_LESS_THAN,
            time_now)
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 2)

        # attribute condition
        attri_key = "aaa"
        cond = timeseries_condition.AttributeMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL, attri_key, attri[attri_key])
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 1)
        self.assert_equal(res.timeseriesMetas[0].attributes, attri)

        cond = timeseries_condition.AttributeMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                                attri_key, attri_key)
        request = metadata.QueryTimeseriesMetaRequest(table_name, cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 0)

        # CompositeMetaQueryCondition
        cond1 = timeseries_condition.DataSourceMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                                key1.data_source)
        cond2 = timeseries_condition.MeasurementMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                                key1.measurement_name)
        composite_cond = timeseries_condition.CompositeMetaQueryCondition(timeseries_condition.MetaQueryCompositeOperator.OP_AND, [cond1, cond2])
        request = metadata.QueryTimeseriesMetaRequest(table_name, composite_cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 1)
        self.assert_equal(res.timeseriesMetas[0].timeseries_key.measurement_name, key1.measurement_name)

        cond1 = timeseries_condition.DataSourceMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL, key1.data_source)
        cond2 = timeseries_condition.MeasurementMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL, key2.measurement_name)
        composite_cond = timeseries_condition.CompositeMetaQueryCondition(
            timeseries_condition.MetaQueryCompositeOperator.OP_AND, [cond1, cond2])
        request = metadata.QueryTimeseriesMetaRequest(table_name, composite_cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 0)

        cond1 = timeseries_condition.DataSourceMetaQueryCondition(timeseries_condition.MetaQuerySingleOperator.OP_EQUAL,
                                                                  key1.data_source)
        cond2 = timeseries_condition.MeasurementMetaQueryCondition(
            timeseries_condition.MetaQuerySingleOperator.OP_EQUAL, key2.measurement_name)
        composite_cond = timeseries_condition.CompositeMetaQueryCondition(
            timeseries_condition.MetaQueryCompositeOperator.OP_OR, [cond1, cond2])
        request = metadata.QueryTimeseriesMetaRequest(table_name, composite_cond)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 2)

        # 测试下meta query的nexttoken
        request = metadata.QueryTimeseriesMetaRequest(table_name, limit =1)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 1)
        request = metadata.QueryTimeseriesMetaRequest(table_name, limit=2, nextToken=res.nextToken)
        res = client.query_timeseries_meta(request)
        self.assert_equal(len(res.timeseriesMetas), 1)
        self.assert_equal(res.nextToken, None)

        client.delete_timeseries_table(table_name)
        print("finish")



if __name__ == '__main__':
    unittest.main()
