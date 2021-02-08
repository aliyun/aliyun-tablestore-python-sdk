# -*- coding: utf8 -*-

from tablestore.metadata import *
import tablestore.protobuf.search_pb2 as search_pb2

class BaseGroupBy(object):
    def __init__(self, field_name, sub_aggs, sub_group_bys, name, type):
        self.field_name = field_name
        self.sub_aggs = sub_aggs
        self.sub_group_bys = sub_group_bys
        self.name = name
        self.type = type

    def add_sub_agg(self, agg):
        self.sub_aggs.append(agg)

    def add_sub_group_by(self, group_by):
        self.sub_group_bys.append(group_by)

    def to_pb_str(self, agg_encode_func, group_by_encode_func, query_encode_func):
        pass

    def _base_to_pb_str(self, proto, agg_encode_func, group_by_encode_func):
        if self.field_name is not None:
            proto.field_name = self.field_name

        if self.sub_aggs is not None:
            agg_encode_func(proto.sub_aggs, self.sub_aggs)

        if self.sub_group_bys is not None:
            group_by_encode_func(proto.sub_group_bys, self.sub_group_bys)

    def range_to_pb(self, ranges, r):
        range_proto = ranges.add()
        begin, end = r[0], r[1]
        if isinstance(begin, (six.integer_types, float)) and isinstance(end, (six.integer_types,float)):
            range_proto.begin = begin
            range_proto.end = end
        else:
            raise OTSClientError('range.begin and range.end must be integer or float')

class GroupKeySort(object):
    def __init__(self, sort_order):
        self.sort_order = sort_order
        
    
class RowCountSort(object):
    def __init__(self, sort_order):
        self.sort_order = sort_order


class SubAggSort(object):
    def __init__(self, sort_order, sub_agg_name):
        self.sort_order = sort_order
        self.sub_agg_name = sub_agg_name

class GroupByField(BaseGroupBy):

    def __init__(self, field_name, size = 10, group_by_sort = None, sub_aggs = [], sub_group_bys = [], name = 'group_by_field'):
        BaseGroupBy.__init__(self, field_name, sub_aggs, sub_group_bys, name, search_pb2.GROUP_BY_FIELD)
        
        self.size = size
        self.group_by_sort = group_by_sort
        self.sub_aggs = sub_aggs
        self.sub_group_bys = sub_group_bys

    def to_pb_str(self, agg_encode_func, group_by_encode_func, query_encode_func):
        proto = search_pb2.GroupByField()
        proto.size = self.size

        if self.group_by_sort is not None and isinstance(self.group_by_sort, list):
            for sort in self.group_by_sort:
                if isinstance(sort, GroupKeySort):
                    sorter = proto.sort.sorters.add()
                    sorter.group_key_sort.order = self._get_enum(sort.sort_order)
                elif isinstance(sort, RowCountSort):
                    sorter = proto.sort.sorters.add()
                    sorter.row_count_sort.order = self._get_enum(sort.sort_order)
                elif isinstance(sort, SubAggSort):
                    sorter = proto.sort.sorters.add()
                    sorter.sub_agg_sort.order = self._get_enum(sort.sort_order)
                    sorter.sub_agg_sort.sub_agg_name = sort.sub_agg_name
                else:
                    raise OTSClientError('Invalid sort type:%s' % str(type(sort)))

        BaseGroupBy._base_to_pb_str(self, proto, agg_encode_func, group_by_encode_func)
        return proto.SerializeToString()

    def _get_enum(self, e):
        # to compatible with enum and enum34
        return e.value if hasattr(e, 'value') else e

class GroupByRange(BaseGroupBy):

    def __init__(self, field_name, ranges, sub_aggs = [], sub_group_bys = [], name = 'group_by_range'):
        BaseGroupBy.__init__(self, field_name, sub_aggs, sub_group_bys, name, search_pb2.GROUP_BY_RANGE)
        self.ranges = ranges

    def add_range(self, range):
        self.ranges.append(range)

    def to_pb_str(self, agg_encode_func, group_by_encode_func, query_encode_func):
        proto = search_pb2.GroupByRange()
        
        if self.ranges is not None and isinstance(self.ranges, list):
            for r in self.ranges:
                if isinstance(r, tuple) and len(r) == 2:
                    self.range_to_pb(proto.ranges, r)
                else:
                    raise OTSClientError('GroupByRange:range must be tuple, and length must equal 2')
        else:
            raise OTSClientError('GroupByRange:ranges must be list')

        BaseGroupBy._base_to_pb_str(self, proto, agg_encode_func, group_by_encode_func)
        return proto.SerializeToString()

class GroupByFilter(BaseGroupBy):
    
    def __init__(self, filters, sub_aggs = [], sub_group_bys = [], name = 'group_by_filter'):
        BaseGroupBy.__init__(self, None, sub_aggs, sub_group_bys, name, search_pb2.GROUP_BY_FILTER)

        self.filters = filters

    def add_filter(self, filter):
        self.filters.append(filter)

    def to_pb_str(self, agg_encode_func, group_by_encode_func, query_encode_func):
        proto = search_pb2.GroupByFilter()
        
        if self.filters is not None and isinstance(self.filters, list):
            for filter in self.filters:
                if isinstance(filter, Query):
                    query_encode_func(proto.filters.add(), filter)
                else:
                    raise OTSClientError('GroupByFilter:filter must be Query')
        else:
            raise OTSClientError('GroupByFilter:filters must be list')

        BaseGroupBy._base_to_pb_str(self, proto, agg_encode_func, group_by_encode_func)
        return proto.SerializeToString()

class GeoPoint(object):
    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon

class GroupByGeoDistance(BaseGroupBy):

    def __init__(self, field_name, origin, ranges, sub_aggs = [], sub_group_bys = [], name = 'group_by_geo_distance'):
        BaseGroupBy.__init__(self, field_name, sub_aggs, sub_group_bys, name, search_pb2.GROUP_BY_GEO_DISTANCE)

        self.origin = origin
        self.ranges = ranges

    def add_range(self, range):
        self.ranges.append(range)

    def to_pb_str(self, agg_encode_func, group_by_encode_func, query_encode_func):
        proto = search_pb2.GroupByGeoDistance()

        if self.origin is not None and isinstance(self.origin, GeoPoint):
            proto.origin.lat = self.origin.lat
            proto.origin.lon = self.origin.lon
        else:
            raise OTSClientError('GroupByGeoDistance:origin must not be None and must be GeoPoint')
        
        if self.ranges is not None and isinstance(self.ranges, list):
            for range in self.ranges:
                if isinstance(range, tuple) and len(range) == 2:
                    self.range_to_pb(proto.ranges, range)
                else:
                    raise OTSClientError('GroupByGeoDistance:range must be tuple, and length must equal 2')
        else:
            raise OTSClientError('GroupByGeoDistance:ranges must be list')

        BaseGroupBy._base_to_pb_str(self, proto, agg_encode_func, group_by_encode_func)
        return proto.SerializeToString()


class GroupByResult(object):
    def __init__(self, name, items):
        self.name = name
        self.items = items

    def addItem(self, group_by_result_item):
        self.items.append(group_by_result_item)

class BaseGroupByResultItem(object):
    def __init__(self, sub_aggs, sub_group_bys):
        self.sub_aggs = sub_aggs
        self.sub_group_bys = sub_group_bys

class GroupByFieldResultItem(BaseGroupByResultItem):
    def __init__(self, key, row_count, sub_aggs, sub_group_bys):
        BaseGroupByResultItem.__init__(self, sub_aggs, sub_group_bys)

        self.key = key
        self.row_count = row_count

class GroupByRangeResultItem(BaseGroupByResultItem):
    def __init__(self, range_from, range_to, row_count, sub_aggs, sub_group_bys):
        BaseGroupByResultItem.__init__(self, sub_aggs, sub_group_bys)

        self.range_from = range_from
        self.range_to = range_to
        self.row_count = row_count

class GroupByFilterResultItem(BaseGroupByResultItem):
    def __init__(self, row_count, sub_aggs, sub_group_bys):
        BaseGroupByResultItem.__init__(self, sub_aggs, sub_group_bys)

        self.row_count = row_count

class GroupByGeoDistanceResultItem(BaseGroupByResultItem):
    def __init__(self, range_from, range_to, row_count, sub_aggs, sub_group_bys):
        BaseGroupByResultItem.__init__(self, sub_aggs, sub_group_bys)

        self.range_from = range_from
        self.range_to = range_to
        self.row_count = row_count
