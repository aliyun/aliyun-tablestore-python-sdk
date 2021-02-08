# -*- coding: utf8 -*-

from tablestore.metadata import *
from tablestore.plainbuffer.plain_buffer_builder import *
import tablestore.protobuf.search_pb2 as search_pb2

class Agg(object):
    
    def __init__(self, field, missing_value, name, agg_type):
        self.field = field
        self.missing = missing_value
        self.name = name
        self.type = agg_type

    def to_pb_str(self, proto):
        agg = proto
        agg.field_name = self.field

        if self.missing is not None:
            agg.missing = bytes(PlainBufferBuilder.serialize_column_value(self.missing))

        return agg.SerializeToString()        

class Max(Agg):

    def __init__(self, field, missing_value = None, name = 'max'):
        Agg.__init__(self, field, missing_value, name, search_pb2.AGG_MAX)

    def to_pb_str(self):
        return Agg.to_pb_str(self, search_pb2.MaxAggregation())


class Min(Agg):

    def __init__(self, field, missing_value = None, name = 'min'):
        Agg.__init__(self, field, missing_value, name, search_pb2.AGG_MIN)

    def to_pb_str(self):
        return Agg.to_pb_str(self, search_pb2.MinAggregation())


class Avg(Agg):

    def __init__(self, field, missing_value = None, name = 'avg'):
        Agg.__init__(self, field, missing_value, name, search_pb2.AGG_AVG)

    def to_pb_str(self):
        return Agg.to_pb_str(self, search_pb2.AvgAggregation())


class Sum(Agg):

    def __init__(self, field, missing_value = None, name = 'sum'):
        Agg.__init__(self, field, missing_value, name, search_pb2.AGG_SUM)

    def to_pb_str(self):
        return Agg.to_pb_str(self, search_pb2.SumAggregation())


class Count(Agg):

    def __init__(self, field, name = 'count'):
        Agg.__init__(self, field, None, name, search_pb2.AGG_COUNT)

    def to_pb_str(self):
        return Agg.to_pb_str(self, search_pb2.SumAggregation())


class DistinctCount(Agg):

    def __init__(self, field, missing_value = None, name = 'distinct_count'):
        Agg.__init__(self, field, missing_value, name, search_pb2.AGG_DISTINCT_COUNT)

    def to_pb_str(self):
        return Agg.to_pb_str(self, search_pb2.DistinctCountAggregation())


"""
TopRows: used in group_by
"""
class TopRows(object):

    def __init__(self, limit, sort, name = 'top_rows'):
        self.limit = limit
        self.sort = sort
        self.name = name
        self.type = search_pb2.AGG_TOP_ROWS

    def to_pb_str(self, encode_sort_func):
        agg = search_pb2.TopRowsAggregation()
        agg.limit = self.limit

        if self.sort is not None:
            for sorter in self.sort.sorters:
                encode_sort_func(agg.sort.sorter.add(), sorter)

        return agg.SerializeToString()

"""
AggreagtionType    ValueType
   Max               double
   Min               double
   Sum               double
   Count             int64
   DistinctCount     int64
   TopRows           [Row]
"""
class AggResult(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value
