# -*- coding: utf8 -*-

import unittest

from timeseries import FlatBufferRows
from tests.lib.api_test_base import APITestBase
import time
from tablestore.flatbuffer import timeseries_flat_buffer_encoder
from tablestore import metadata
from tablestore import *

class TimeseriesFlatBufferTest(APITestBase):


    """TimeseriesFlatBufferTest"""

    def test_flat_buffer_encode(self):
        """测试flatbuffer编码正确性"""
        tags = {"tag1": "t1", "tag2": "t2"}
        field1 = {"long_field": 1, "string_field": "string", "bool_field": True, "doubel_field": 0.3, "doubel_field2":0.4}
        field2 = {"long_field2": 3, "string_field2": "string2", "doubel_field2": 0.4, "byte_field": bytearray(b'abc1')}
        key2 = metadata.TimeseriesKey("measure2", "datasource2", tags)
        key1 = metadata.TimeseriesKey("measure1", "datasource1", tags)
        time1 = time.time()
        row1 = metadata.TimeseriesRow(key1, field1, int(time1))
        time2 = time.time()
        row2 = metadata.TimeseriesRow(key2, field2, int(time2))
        rows = [row1, row2]

        resultbyte = timeseries_flat_buffer_encoder.get_column_val_by_tp("flatbuffertest", rows)

        flatBufferRows = FlatBufferRows.FlatBufferRows.GetRootAsFlatBufferRows(resultbyte)

        self.assert_equal(flatBufferRows.RowGroupsLength(), len(rows))

        decode_row1 = flatBufferRows.RowGroups(0)
        self.assert_equal(decode_row1.FieldNamesLength(), len(field1))
        self.assert_equal(decode_row1.FieldTypesLength(), len(field1))
        self.assert_equal(decode_row1.MeasurementName(), "measure1")
        self.assert_equal(decode_row1.RowsLength(), 1)
        row_in_group1 = decode_row1.Rows(0)
        self.assert_equal(row_in_group1.DataSource(), "datasource1")
        self.assert_equal(row_in_group1.TagListLength(), len(tags))
        self.assert_equal(row_in_group1.TagList(0).Name(), "tag1")
        self.assert_equal(row_in_group1.TagList(0).Value(), "t1")
        self.assert_equal(row_in_group1.Time(), int(time1))
        self.assert_equal(row_in_group1.FieldValues().StringValues(0), "string")
        self.assert_equal(row_in_group1.FieldValues().LongValues(0), 1)
        self.assert_equal(row_in_group1.FieldValues().BoolValues(0), True)
        self.assert_equal(row_in_group1.FieldValues().DoubleValues(0), 0.3)
        self.assert_equal(row_in_group1.FieldValues().DoubleValues(1), 0.4)

        decode_row2 = flatBufferRows.RowGroups(1)
        self.assert_equal(decode_row2.FieldNamesLength(), len(field2))
        self.assert_equal(decode_row2.FieldTypesLength(), len(field2))
        self.assert_equal(decode_row2.MeasurementName(), "measure2")
        row_in_group2 = decode_row2.Rows(0)
        self.assert_equal(row_in_group2.DataSource(), "datasource2")
        self.assert_equal(row_in_group2.FieldValues().BinaryValues(0).ValueAsNumpy().tobytes(), b'abc1')


        tags = {"tag1": "t1", "tag2": "t2"}
        field1 = {"long_field": 1, "string_field": "string", "bool_field": True, "doubel_field": 0.3,
                  "doubel_field2": 0.4, "string_field2": "string", "string_field3": "string",
                  "byte_field1": bytearray(b'abc1'), "byte_field2": bytearray(b'abc2')}
        key1 = metadata.TimeseriesKey("measure1", "datasource1", tags)
        time1 = time.time()
        row1 = metadata.TimeseriesRow(key1, field1, int(time1))
        rows = [row1]
        resultbyte = timeseries_flat_buffer_encoder.get_column_val_by_tp("flatbuffertest", rows)
        flatBufferRows = FlatBufferRows.FlatBufferRows.GetRootAsFlatBufferRows(resultbyte)
        self.assert_equal(flatBufferRows.RowGroupsLength(), len(rows))

        decode_row1 = flatBufferRows.RowGroups(0)
        self.assert_equal(decode_row1.FieldNamesLength(), len(field1))
        self.assert_equal(decode_row1.FieldTypesLength(), len(field1))
        self.assert_equal(decode_row1.MeasurementName(), "measure1")
        self.assert_equal(decode_row1.RowsLength(), 1)
        row_in_group1 = decode_row1.Rows(0)
        self.assert_equal(row_in_group1.DataSource(), "datasource1")
        self.assert_equal(row_in_group1.TagListLength(), len(tags))
        self.assert_equal(row_in_group1.TagList(0).Name(), "tag1")
        self.assert_equal(row_in_group1.TagList(0).Value(), "t1")
        self.assert_equal(row_in_group1.Time(), int(time1))

        self.assert_equal(row_in_group1.FieldValues().DoubleValuesLength(), 2)
        self.assert_equal(row_in_group1.FieldValues().StringValuesLength(), 3)
        self.assert_equal(row_in_group1.FieldValues().BinaryValuesLength(), 2)
        self.assert_equal(row_in_group1.FieldValues().LongValuesLength(), 1)
        self.assert_equal(row_in_group1.FieldValues().BoolValuesLength(), 1)

        self.assert_equal(row_in_group1.FieldValues().StringValues(0), field1["string_field"])
        self.assert_equal(row_in_group1.FieldValues().StringValues(1), field1["string_field2"])
        self.assert_equal(row_in_group1.FieldValues().StringValues(2), field1["string_field3"])
        self.assert_equal(row_in_group1.FieldValues().LongValues(0), 1)
        self.assert_equal(row_in_group1.FieldValues().BoolValues(0), True)
        self.assert_equal(row_in_group1.FieldValues().DoubleValues(0), 0.3)
        self.assert_equal(row_in_group1.FieldValues().BinaryValues(0).ValueAsNumpy().tobytes(), bytes(field1["byte_field1"]))

if __name__ == '__main__':
    unittest.main()
