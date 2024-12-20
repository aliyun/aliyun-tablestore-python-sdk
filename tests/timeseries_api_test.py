# -*- coding: utf8 -*-
import unittest


from tests.lib.api_test_base import APITestBase
import time
from tablestore import metadata


class TimeseriesApiTest(APITestBase):
    """TimeseriesApiTest"""

    def test_timeseries_api(self):
        client = self.client_test
        prefix = "python_sdk_test"
        table_name = prefix + str(int(time.time()))

        try:
            # 清理环境
            b = client.list_timeseries_table()
            for item in b:
                if item.timeseries_table_name.startswith(prefix):
                    client.delete_timeseries_table(item.timeseries_table_name)
        except Exception as e:
            print(e)

        print("start to sleep")
        time.sleep(25)
        print("finish sleep")

        # 建表
        meta = metadata.TimeseriesTableMeta(table_name)
        meta.field_primary_keys = [('a1', 'INTEGER'), ('b1', 'STRING')]
        meta.timeseries_keys = ["a", "b"]
        meta.timeseries_table_options = metadata.TimeseriesTableOptions(172800)
        request = metadata.CreateTimeseriesTableRequest(meta)
        client.create_timeseries_table(request)

        self.logger.info("start to sleep")
        time.sleep(25)
        self.logger.info("finish sleep")

        b = client.list_timeseries_table()
        self.assertTrue(len(b) > 0, "num of timeseries table should be more than one")

        for item in b:
            if item.timeseries_table_name == table_name:
                table_item = item
                found = True
        self.assertTrue(found, "do not find expected table")

        self.assert_equal(table_item.timeseries_table_options.time_to_live, 172800)
        self.assert_equal(len(table_item.timeseries_keys), 2)
        self.assert_equal(table_item.timeseries_keys[0], meta.timeseries_keys[0])
        self.assert_equal(table_item.timeseries_keys[1], meta.timeseries_keys[1])
        i = 0
        while i < 5:
            try:
                resp = client.describe_timeseries_table(table_name)
                break
            except Exception as e:
                i=i+1
                time.sleep(20)

        self.assert_equal(resp.table_meta.timeseries_table_name, table_name)
        self.assert_equal(resp.table_meta.timeseries_table_options.time_to_live, 172800)
        self.assert_equal(len(resp.table_meta.timeseries_keys), 2)
        self.assert_equal(resp.table_meta.timeseries_keys[0], meta.timeseries_keys[0])
        self.assert_equal(resp.table_meta.timeseries_keys[1], meta.timeseries_keys[1])
        self.assert_equal(resp.table_meta.status, "CREATED")

        time_to_live = 172801
        tableOption = metadata.TimeseriesTableOptions(time_to_live)
        tableMeta = metadata.TimeseriesTableMeta(table_name, tableOption)
        client.update_timeseries_table(tableMeta)

        time.sleep(10)
        resp = client.describe_timeseries_table(table_name)
        self.assert_equal(resp.table_meta.timeseries_table_options.time_to_live, time_to_live)

        client.delete_timeseries_table(table_name)

        table_name = prefix + str(int(time.time()))
        meta = metadata.TimeseriesTableMeta(table_name)
        meta.timeseries_table_options = metadata.TimeseriesTableOptions(172800)
        request = metadata.CreateTimeseriesTableRequest(meta)
        client.create_timeseries_table(request)
        time.sleep(25)
        # puttimeseriesdata
        tags = {"tag1": "t1", "tag2": "t2"}
        field1 = {"long_field": 1, "string_field": "string", "bool_field": True, "double_field": 0.3}
        field2 = {"string_field": b'a', "b1": bytearray(b'this is byte'), "b2": bytearray(b'this is byte2')}
        key2 = metadata.TimeseriesKey("measure2", "datasource2", tags)
        key1 = metadata.TimeseriesKey("measure1", "datasource1", tags)
        time1 = time.time()
        row1 = metadata.TimeseriesRow(key1, field1, int(time1 * 1000000))
        time2 = time.time()
        row2 = metadata.TimeseriesRow(key2, field2, int(time2 * 1000000))
        time3 = time.time()
        row3 = metadata.TimeseriesRow(key2, field2, int(time3 * 1000000))
        rows = [row1, row2, row3]
        client.put_timeseries_data(table_name, rows)

        # gettimeseriesdata
        request = metadata.GetTimeseriesDataRequest(table_name)
        request.timeseriesKey = key2
        request.endTimeInUs = int(time.time() * 1000000)
        resp = client.get_timeseries_data(request)

        self.assert_equal(len(resp.rows), 2)
        self.assert_equal(resp.rows[0].fields["string_field"], field2["string_field"])
        self.assert_equal(resp.rows[0].fields["b1"], field2["b1"])
        self.assert_equal(resp.rows[0].fields["b2"], field2["b2"])
        self.assert_equal(resp.rows[0].timeseries_key.measurement_name, key2.measurement_name)
        self.assert_equal(resp.rows[0].timeseries_key.data_source, key2.data_source)
        self.assert_equal(resp.rows[0].timeseries_key.tags, tags)

        request = metadata.GetTimeseriesDataRequest(table_name)
        request.timeseriesKey = key1
        request.endTimeInUs = int(time.time() * 1000000)
        resp = client.get_timeseries_data(request)

        self.assert_equal(len(resp.rows), 1)
        self.assert_equal(resp.rows[0].fields["long_field"], field1["long_field"])
        self.assert_equal(resp.rows[0].fields["string_field"], field1["string_field"])
        self.assert_equal(resp.rows[0].fields["bool_field"], field1["bool_field"])
        self.assert_equal(resp.rows[0].fields["double_field"], field1["double_field"])
        self.assert_equal(resp.rows[0].timeseries_key.measurement_name, key1.measurement_name)
        self.assert_equal(resp.rows[0].timeseries_key.data_source, key1.data_source)
        self.assert_equal(resp.rows[0].timeseries_key.tags, tags)

        # update meta
        attri = {"aaa": "bbb", "ccc": "dddd"}
        meta = metadata.TimeseriesMeta(key1, attri)
        req = metadata.UpdateTimeseriesMetaRequest(table_name, [meta])
        client.update_timeseries_meta(req)

        req = metadata.DeleteTimeseriesMetaRequest(table_name, [key2])
        client.delete_timeseries_meta(req)

        time.sleep(60)
        request = metadata.QueryTimeseriesMetaRequest(table_name, getTotalHits=True)
        res = client.query_timeseries_meta(request)
        self.assert_equal(res.totalHits, 1)
        self.assert_equal(res.timeseriesMetas[0].timeseries_key.measurement_name, key1.measurement_name)
        self.assert_equal(res.timeseriesMetas[0].timeseries_key.data_source, key1.data_source)
        self.assert_equal(res.timeseriesMetas[0].timeseries_key.tags, key1.tags)
        self.assert_equal(res.timeseriesMetas[0].attributes, attri)


        ## 测试一下中文
        field1 = {"string_field": "数值1", "string_field2": "数值2"}
        tags1 = {"tag1": "标签1", "tag2": "标签2"}
        key1 = metadata.TimeseriesKey("测试1", "来源1", tags1)
        row1 = metadata.TimeseriesRow(key1, field1, int(time.time() * 1000000))
        rows = [row1]
        client.put_timeseries_data(table_name, rows)
        request = metadata.GetTimeseriesDataRequest(table_name)
        request.timeseriesKey = key1
        request.endTimeInUs = int(time.time() * 1000000)
        resp = client.get_timeseries_data(request)

        self.assert_equal(resp.rows[0].fields["string_field"], field1["string_field"])
        self.assert_equal(resp.rows[0].fields["string_field2"], field1["string_field2"])
        self.assert_equal(resp.rows[0].timeseries_key.measurement_name, key1.measurement_name)
        self.assert_equal(resp.rows[0].timeseries_key.data_source, key1.data_source)
        self.assert_equal(resp.rows[0].timeseries_key.tags, tags1)

        field1 = {"string_field0": "温度.abc", "string_field1": "_@?%$A", "string_field2": "\t\n", "string_field3": "\""}
        tags1 = {"tag1": "标签1", "tag2": "标签2"}
        key1 = metadata.TimeseriesKey("测试1", "来源1", tags1)
        row1 = metadata.TimeseriesRow(key1, field1, int(time.time() * 1000000))
        rows = [row1]
        client.put_timeseries_data(table_name, rows)
        request = metadata.GetTimeseriesDataRequest(table_name)
        request.timeseriesKey = key1
        request.endTimeInUs = int(time.time() * 1000000)
        resp = client.get_timeseries_data(request)

        self.assert_equal(resp.rows[1].fields["string_field0"], field1["string_field0"])
        self.assert_equal(resp.rows[1].fields["string_field1"], field1["string_field1"])
        self.assert_equal(resp.rows[1].fields["string_field2"], field1["string_field2"])
        self.assert_equal(resp.rows[1].fields["string_field3"], field1["string_field3"])
        self.assert_equal(resp.rows[1].timeseries_key.measurement_name, key1.measurement_name)
        self.assert_equal(resp.rows[1].timeseries_key.data_source, key1.data_source)
        self.assert_equal(resp.rows[1].timeseries_key.tags, tags1)

        print("begin to delete table")

        client.delete_timeseries_table(table_name)
        try:
            resp = client.describe_timeseries_table(table_name)
            self.fail("fail")
        except Exception as e:
            print("1")
        print("finish")

    def test_timeseries_getdata_nexttoken_test(self):
        client = self.client_test
        prefix = "python_sdk_nt_test"
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
        key1 = metadata.TimeseriesKey("measure1", "datasource1", tags)
        for i in range(1, 20):
            time1 = time.time()
            row1 = metadata.TimeseriesRow(key1, field1, int(time1 * 1000000))
            client.put_timeseries_data(table_name, [row1])

        request = metadata.GetTimeseriesDataRequest(table_name, key1)
        request.endTimeInUs = int(time.time() * 1000000)
        request.limit = 10
        resp = client.get_timeseries_data(request)
        self.assert_equal(len(resp.rows), 10)
        self.assertTrue(resp.nextToken is not None)

        request.nextToken = resp.nextToken
        resp = client.get_timeseries_data(request)
        self.assert_equal(len(resp.rows), 9)
        self.assert_equal(resp.nextToken, None)

        client.delete_timeseries_table(table_name)

    def test_user_define_primary_key(self):
        client = self.client_test
        prefix = "python_sdk_definekey_test"
        table_name = prefix + str(int(time.time()))

        try:
            # 清理环境
            b = client.list_timeseries_table()
            for item in b:
                if item.timeseries_table_name.startswith(prefix):
                    client.delete_timeseries_table(item.timeseries_table_name)
        except Exception as e:
            print(e)

        tableOptitable_optionn = metadata.TimeseriesTableOptions(172800)
        meta_option = metadata.TimeseriesMetaOptions(None, False)
        timeseries_keys = ["a", "b"]
        field_primary_keys = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
        table_meta = metadata.TimeseriesTableMeta(table_name, tableOptitable_optionn, meta_option, timeseries_keys, field_primary_keys)
        request = metadata.CreateTimeseriesTableRequest(table_meta)
        client.create_timeseries_table(request)

        # 写入数据
        tags = {"a": "a1", "b": "b1"}
        field1 = {"gid": 1, "uid": 2, "bool_field": True, "double_field": 0.3}
        field2 = {"bool_field": True, "double_field": 0.3}
        key1 = metadata.TimeseriesKey(tags=tags)
        for i in range(1, 20):
            time1 = time.time()
            row1 = metadata.TimeseriesRow(key1, field1, int(time1 * 1000000))
            client.put_timeseries_data(table_name, [row1])
            
        try:
            client.put_timeseries_data(table_name, [metadata.TimeseriesRow(key1, field2, int(time.time() * 1000000))])
            self.fail("should fail but not.")
        except Exception as e:
            self.assertTrue(str(e).index("missing primary key fields") > 0)
        # 读取数据
        limit = 1
        request = metadata.GetTimeseriesDataRequest(table_name, key1, limit=limit)
        request.endTimeInUs = int(time.time() * 1000000)
        resp = client.get_timeseries_data(request)
        self.assert_equal(len(resp.rows), limit)
        self.assert_equal(resp.rows[0].timeseries_key.measurement_name, None)
        self.assert_equal(resp.rows[0].timeseries_key.data_source, None)
        self.assert_equal(resp.rows[0].timeseries_key.tags, tags)
        self.assert_equal(resp.rows[0].fields["gid"], field1["gid"])
        self.assert_equal(resp.rows[0].fields["uid"], field1["uid"])
        self.assert_equal(resp.rows[0].fields["bool_field"], field1["bool_field"])
        self.assert_equal(resp.rows[0].fields["double_field"], field1["double_field"])

        # 删表
        client.delete_timeseries_table(table_name)



if __name__ == '__main__':
    unittest.main()
