# -*- coding: utf8 -*-

import six
from tablestore.error import *
from enum import Enum
import tablestore.protobuf.search_pb2 as search_pb2

class TableMeta(object):

    def __init__(self, table_name, schema_of_primary_key, defined_columns=[]):
        # schema_of_primary_key: [('PK0', 'STRING'), ('PK1', 'INTEGER'), ...]
        self.table_name = table_name
        self.schema_of_primary_key = schema_of_primary_key
        self.defined_columns = defined_columns

class TableOptions(object):
    def __init__(self, time_to_live = -1, max_version = 1, max_time_deviation = 86400):
        self.time_to_live = time_to_live
        self.max_version = max_version
        self.max_time_deviation = max_time_deviation

class CapacityUnit(object):

    def __init__(self, read=0, write=0):
        self.read = read
        self.write = write


class ReservedThroughput(object):

    def __init__(self, capacity_unit):
        self.capacity_unit = capacity_unit


class ReservedThroughputDetails(object):

    def __init__(self, capacity_unit, last_increase_time, last_decrease_time):
        self.capacity_unit = capacity_unit
        self.last_increase_time = last_increase_time
        self.last_decrease_time = last_decrease_time


class FieldType(Enum):
    LONG = search_pb2.LONG
    DOUBLE = search_pb2.DOUBLE
    BOOLEAN = search_pb2.BOOLEAN
    KEYWORD = search_pb2.KEYWORD
    TEXT = search_pb2.TEXT
    NESTED = search_pb2.NESTED
    GEOPOINT = search_pb2.GEO_POINT

class AnalyzerType(object):
    SINGLEWORD = "single_word"
    MAXWORD = "max_word"

class ScoreMode(Enum):
    NONE = search_pb2.SCORE_MODE_NONE
    AVG = search_pb2.SCORE_MODE_AVG
    MAX = search_pb2.SCORE_MODE_MAX
    TOTAL = search_pb2.SCORE_MODE_TOTAL
    MIN = search_pb2.SCORE_MODE_MIN

class SortMode(Enum):
    MIN = search_pb2.SORT_MODE_MIN
    MAX = search_pb2.SORT_MODE_MAX
    AVG = search_pb2.SORT_MODE_AVG

class SortOrder(Enum):
    ASC = search_pb2.SORT_ORDER_ASC
    DESC = search_pb2.SORT_ORDER_DESC

class GeoDistanceType(Enum):
    ARC = search_pb2.GEO_DISTANCE_ARC
    PLANE = search_pb2.GEO_DISTANCE_PLANE

class Sorter(object):
    def __init__(self):
        pass

class FieldSort(Sorter):

    def __init__(self, field_name, sort_order=None, sort_mode=None, nested_filter=None):
        self.field_name = field_name
        self.sort_order = sort_order
        self.sort_mode = sort_mode
        self.nested_filter = nested_filter

class GeoDistanceSort(Sorter):

    def __init__(self, field_name, points, sort_order=None, sort_mode=None, geo_distance_type=None, nested_filter=None):
        self.field_name = field_name
        self.points = points
        self.geo_distance_type = geo_distance_type
        self.sort_order = sort_order
        self.sort_mode = sort_mode
        self.nested_filter = nested_filter

class ScoreSort(Sorter):

    def __init__(self, sort_order=SortOrder.DESC):
        self.sort_order = sort_order

class PrimaryKeySort(Sorter):

    def __init__(self, sort_order=SortOrder.ASC):
        self.sort_order = sort_order

class Sort(object):

    def __init__(self, sorters):
        self.sorters = sorters

class IndexSetting(object):

    def __init__(self, routing_fields=[]):
        self.routing_fields = routing_fields


class FieldSchema(object):

    def __init__(self, field_name, field_type, index=None,
        store=None, is_array=None, enable_sort_and_agg=None,
        analyzer=None, sub_field_schemas=[]):
        self.field_name = field_name
        self.field_type = field_type
        self.index = index
        self.store = store
        self.is_array = is_array
        self.enable_sort_and_agg = enable_sort_and_agg
        self.analyzer = analyzer
        self.sub_field_schemas = sub_field_schemas

class SyncPhase(Enum):
    FULL = 0
    INCR = 1

class SyncStat(object):

    def __init__(self, sync_phase, current_sync_timestamp):
        self.sync_phase = sync_phase
        self.current_sync_timestamp = current_sync_timestamp

class SearchIndexMeta(object):

    def __init__(self, fields, index_setting=None, index_sort=None):
        self.fields = fields
        self.index_setting = index_setting
        self.index_sort = index_sort

class DefinedColumnSchema(object):

    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type

class SecondaryIndexType(Enum):
    GLOBAL_INDEX = 0
    LOCAL_INDEX = 1

class SecondaryIndexMeta(object):

    def __init__(self, index_name, primary_key_names, defined_column_names, index_type=SecondaryIndexType.GLOBAL_INDEX):
        self.index_name = index_name
        self.primary_key_names = primary_key_names
        self.defined_column_names = defined_column_names
        self.index_type = index_type

class ColumnType(object):
    STRING = "STRING"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    DOUBLE = "DOUBLE"
    BINARY = "BINARY"
    INF_MIN = "INF_MIN"
    INF_MAX = "INF_MAX"

class ReturnType(object):
    RT_NONE = "RT_NONE"
    RT_PK = "RT_PK"

class Column(object):
    def __init__(self, name, value = None, timestamp = None):
        self.name = name
        self.value = value
        self.timestamp = timestamp

    def set_timestamp(self, timestamp):
        self.timestamp = timestamp

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def get_timestamp(self):
        return self.timestamp

class Direction(object):
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"

class UpdateType(object):
    PUT = "PUT"
    DELETE = "DELETE"
    DELETE_ALL = "DELETE_ALL"
    INCREMENT = "INCREMENT"

class UpdateTableResponse(object):

    def __init__(self, reserved_throughput_details, table_options):
        self.reserved_throughput_details = reserved_throughput_details
        self.table_options = table_options


class DescribeTableResponse(object):

    def __init__(self, table_meta, table_options, reserved_throughput_details, secondary_indexes=[]):
        self.table_meta = table_meta
        self.table_options = table_options
        self.reserved_throughput_details = reserved_throughput_details
        self.secondary_indexes = secondary_indexes


class RowDataItem(object):

    def __init__(self, is_ok, error_code, error_message, table_name, consumed, primary_key_columns, attribute_columns):
        # is_ok can be True or False
        # when is_ok is False,
        #     error_code & error_message are available
        # when is_ok is True,
        #     consumed & primary_key_columns & attribute_columns are available
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.table_name = table_name
        self.consumed = consumed
        self.row = None
        if primary_key_columns is not None or attribute_columns is not None:
            self.row = Row(primary_key_columns, attribute_columns)

class LogicalOperator(object):
    NOT = 0
    AND = 1
    OR = 2

    __values__ = [
        NOT,
        AND,
        OR
    ]

    __members__ = [
        "LogicalOperator.NOT",
        "LogicalOperator.AND",
        "LogicalOperator.OR"
    ]

class ComparatorType(object):
    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER_EQUAL = 3
    LESS_THAN = 4
    LESS_EQUAL = 5

    __values__ = [
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_EQUAL,
        LESS_THAN,
        LESS_EQUAL,
    ]

    __members__ = [
        "ComparatorType.EQUAL",
        "ComparatorType.NOT_EQUAL",
        "ComparatorType.GREATER_THAN",
        "ComparatorType.GREATER_EQUAL",
        "ComparatorType.LESS_THAN",
        "ComparatorType.LESS_EQUAL",
    ]


class ColumnConditionType(object):
    COMPOSITE_COLUMN_CONDITION = 0
    SINGLE_COLUMN_CONDITION = 1

class ColumnCondition(object):
    pass

class CompositeColumnCondition(ColumnCondition):

    def __init__(self, combinator):
        self.sub_conditions = []
        self.set_combinator(combinator)

    def get_type(self):
        return ColumnConditionType.COMPOSITE_COLUMN_CONDITION

    def set_combinator(self, combinator):
        if combinator not in LogicalOperator.__values__:
            raise OTSClientError(
                "Expect input combinator should be one of %s, but '%s'"%(str(LogicalOperator.__members__), combinator)
            )
        self.combinator = combinator

    def get_combinator(self):
        return self.combinator

    def add_sub_condition(self, condition):
        if not isinstance(condition, ColumnCondition):
            raise OTSClientError(
                "The input condition should be an instance of ColumnCondition, not %s"%
                condition.__class__.__name__
            )

        self.sub_conditions.append(condition)

    def clear_sub_condition(self):
        self.sub_conditions = []

class SingleColumnCondition(ColumnCondition):

    def __init__(self, column_name, column_value, comparator, pass_if_missing = True, latest_version_only = True):
        self.column_name = column_name
        self.column_value = column_value

        self.comparator = None
        self.pass_if_missing = None
        self.latest_version_only = None

        self.set_comparator(comparator)
        self.set_pass_if_missing(pass_if_missing)
        self.set_latest_version_only(latest_version_only)

    def get_type(self):
        return ColumnConditionType.SINGLE_COLUMN_CONDITION

    def set_pass_if_missing(self, pass_if_missing):
        """
        设置```pass_if_missing```

        由于OTS一行的属性列不固定，有可能存在有condition条件的列在该行不存在的情况，这时
        参数控制在这种情况下对该行的检查结果。
        如果设置为True，则若列在该行中不存在，则检查条件通过。
        如果设置为False，则若列在该行中不存在，则检查条件失败。
        默认值为True。
        """
        if not isinstance(pass_if_missing, bool):
            raise OTSClientError(
                "The input pass_if_missing of SingleColumnCondition should be an instance of Bool, not %s"%
                pass_if_missing.__class__.__name__
            )
        self.pass_if_missing = pass_if_missing

    def get_pass_if_missing(self):
        return self.pass_if_missing

    def set_latest_version_only(self, latest_version_only):
        if not isinstance(latest_version_only, bool):
            raise OTSClientError(
                "The input latest_version_only of SingleColumnCondition should be an instance of Bool, not %s"%
                latest_version_only.__class__.__name__
            )
        self.latest_version_only = latest_version_only

    def get_latest_version_only(self):
        return self.latest_version_only

    def set_column_name(self, column_name):
        if not isinstance(column_name, (six.text_type, six.binary_type)):
            raise OTSClientError(
                    "The input column_name of SingleColumnCondition should be an instance of str, not %s"%
                    column_name.__class__.__name__
                    )
        if column_name is None:
            raise OTSClientError("The input column_name of SingleColumnCondition should not be None")
        self.column_name = column_name

    def get_column_name(self):
        return self.column_name

    def set_column_value(self, column_value):
        if column_value is None:
            raise OTSClientError("The input column_value of SingleColumnCondition should not be None")

        self.column_value = column_value

    def get_column_value(self):
        return self.column_value

    def set_comparator(self, comparator):
        if comparator not in ComparatorType.__values__:
            raise OTSClientError(
                "Expect input comparator of SingleColumnCondition should be one of %s, but '%s'" %
                (str(ComparatorType.__members__), comparator)
            )
        self.comparator = comparator

    def get_comparator(self):
        return self.comparator

class RowExistenceExpectation(object):
    IGNORE = "IGNORE"
    EXPECT_EXIST = "EXPECT_EXIST"
    EXPECT_NOT_EXIST = "EXPECT_NOT_EXIST"

    __values__ = [
        IGNORE,
        EXPECT_EXIST,
        EXPECT_NOT_EXIST,
    ]

    __members__ = [
        "RowExistenceExpectation.IGNORE",
        "RowExistenceExpectation.EXPECT_EXIST",
        "RowExistenceExpectation.EXPECT_NOT_EXIST",
    ]

class Condition(object):

    def __init__(self, row_existence_expectation, column_condition = None):
        self.row_existence_expectation = None
        self.column_condition = None

        self.set_row_existence_expectation(row_existence_expectation)
        if column_condition != None:
            self.set_column_condition(column_condition)

    def set_row_existence_expectation(self, row_existence_expectation):
        if row_existence_expectation not in RowExistenceExpectation.__values__:
            raise OTSClientError(
                "Expect input row_existence_expectation should be one of %s, but '%s'"%(str(RowExistenceExpectation.__members__), row_existence_expectation)
            )

        self.row_existence_expectation = row_existence_expectation

    def get_row_existence_expectation(self):
        return self.row_existence_expectation

    def set_column_condition(self, column_condition):
        if not isinstance(column_condition, ColumnCondition):
            raise OTSClientError(
                "The input column_condition should be an instance of ColumnCondition, not %s"%
                column_condition.__class__.__name__
            )
        self.column_condition = column_condition

    def get_column_condition(self):
        self.column_condition

class Row(object):
    def __init__(self, primary_key, attribute_columns = None):
        self.primary_key = primary_key
        self.attribute_columns = attribute_columns

class RowItem(object):

    def __init__(self, row_type, row, condition, return_type = None):
        self.type = row_type
        self.condition = condition
        self.row = row
        self.return_type = return_type

class PutRowItem(RowItem):

    def __init__(self, row, condition, return_type = None):
        super(PutRowItem, self).__init__(BatchWriteRowType.PUT, row, condition, return_type)

class UpdateRowItem(RowItem):

    def __init__(self, row, condition, return_type = None):
        super(UpdateRowItem, self).__init__(BatchWriteRowType.UPDATE, row, condition, return_type)

class DeleteRowItem(RowItem):

    def __init__(self, row, condition, return_type = None):
        super(DeleteRowItem, self).__init__(BatchWriteRowType.DELETE, row, condition, return_type)


class TableInBatchGetRowItem(object):

    def __init__(self, table_name, primary_keys, columns_to_get=None,
                 column_filter=None, max_version=None, time_range=None,
                 start_column=None, end_column=None, token=None):
        self.table_name = table_name
        self.primary_keys = primary_keys
        self.columns_to_get = columns_to_get
        self.column_filter = column_filter
        self.max_version = max_version
        self.time_range = time_range
        self.start_column = start_column
        self.end_column = end_column
        self.token = token


class BatchGetRowRequest(object):

    def __init__(self):
        self.items = {}

    def add(self, table_item):
        """
        说明：添加tablestore.metadata.TableInBatchGetRowItem对象
        注意：对象内部存储tablestore.metadata.TableInBatchGetRowItem对象采用‘字典’的形式，Key是表
              的名字，因此如果插入同表名的对象，那么之前的对象将被覆盖。
        """
        if not isinstance(table_item, TableInBatchGetRowItem):
            raise OTSClientError(
                "The input table_item should be an instance of TableInBatchGetRowItem, not %s"%
                table_item.__class__.__name__
            )

        self.items[table_item.table_name] = table_item

class BatchGetRowResponse(object):

    def __init__(self, response):
        self.items = {}

        for rows in response:
            for row in rows:
                table_name = row.table_name
                result_rows = self.items.get(table_name)
                if result_rows == None:
                    self.items[table_name] = [row]
                else:
                    result_rows.append(row)

    def get_failed_rows(self):
        succ, fail = self.get_result()
        return fail

    def get_succeed_rows(self):
        succ, fail = self.get_result()
        return succ

    def get_result(self):
        succ = []
        fail = []
        for rows in list(self.items.values()):
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_result_by_table(self, table_name):
        return self.items.get(table_name)

    def is_all_succeed(self):
        return len(self.get_failed_rows()) == 0

class BatchWriteRowType(object):
    PUT = "put"
    UPDATE = "update"
    DELETE = "delete"


class TableInBatchWriteRowItem(object):

    def __init__(self, table_name, row_items):
        self.table_name = table_name
        self.row_items = row_items


class BatchWriteRowRequest(object):

    def __init__(self):
        self.items = {}
        self.transaction_id = None

    def add(self, table_item):
        """
        说明：添加tablestore.metadata.TableInBatchWriteRowItem对象
        注意：对象内部存储tablestore.metadata.TableInBatchWriteRowItem对象采用‘字典’的形式，Key是表
              的名字，因此如果插入同表名的对象，那么之前的对象将被覆盖。
        """
        if not isinstance(table_item, TableInBatchWriteRowItem):
            raise OTSClientError(
                "The input table_item should be an instance of TableInBatchWriteRowItem, not %s"%
                table_item.__class__.__name__
            )

        self.items[table_item.table_name] = table_item

    def set_transaction_id(self, transcation_id):
        self.transaction_id = transcation_id


class BatchWriteRowResponse(object):

    def __init__(self, request, response):
        self.table_of_put = {}
        self.table_of_update = {}
        self.table_of_delete = {}

        for table_name in list(response.keys()):
            put_list = []
            update_list = []
            delete_list = []
            for index in range(len(response[table_name])):
                row_item = response[table_name][index]
                request_row = request.items[table_name].row_items[index]
                if request_row.type == BatchWriteRowType.PUT:
                    put_list.append(row_item)
                elif request_row.type == BatchWriteRowType.UPDATE:
                    update_list.append(row_item)
                else:
                    delete_list.append(row_item)
            self.table_of_put[table_name] = put_list
            self.table_of_update[table_name] = update_list
            self.table_of_delete[table_name] = delete_list

    def get_put(self):
        succ = []
        fail = []

        for rows in list(self.table_of_put.values()):
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_put_by_table(self, table_name):
        return self.table_of_put[table_name]

    def get_failed_of_put(self):
        succ, fail = self.get_put()
        succ = None
        return fail

    def get_succeed_of_put(self):
        succ, fail = self.get_put()
        fail = None
        return succ

    def get_update(self):
        succ = []
        fail = []

        for rows in list(self.table_of_update.values()):
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_update_by_table(self, table_name):
        return self.table_of_update[table_name]

    def get_failed_of_update(self):
        succ, fail = self.get_update()
        succ = None
        return fail

    def get_succeed_of_update(self):
        succ, fail = self.get_update()
        fail = None
        return succ

    def get_delete(self):
        succ = []
        fail = []

        for rows in list(self.table_of_delete.values()):
            for row in rows:
                if row.is_ok:
                    succ.append(row)
                else:
                    fail.append(row)

        return succ, fail

    def get_delete_by_table(self, table_name):
        return self.table_of_delete[table_name]

    def get_failed_of_delete(self):
        succ, fail = self.get_delete()
        succ = None
        return fail

    def get_succeed_of_delete(self):
        succ, fail = self.get_delete()
        fail = None
        return succ

    def is_all_succeed(self):
        return len(self.get_failed_of_put()) == 0 and len(self.get_failed_of_update()) == 0 and len(self.get_failed_of_delete()) == 0

class BatchWriteRowResponseItem(object):

    def __init__(self, is_ok, error_code, error_message, consumed, primary_key):
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.consumed = consumed
        self.row = Row(primary_key)


class INF_MIN(object):
    # for get_range
    pass


class INF_MAX(object):
    # for get_range
    pass

class PK_AUTO_INCR(object):
    # for put_row
    pass

class QueryType(Enum):
    MATCH_QUERY = search_pb2.MATCH_QUERY
    MATCH_PHRASE_QUERY = search_pb2.MATCH_PHRASE_QUERY
    TERM_QUERY = search_pb2.TERM_QUERY
    RANGE_QUERY = search_pb2.RANGE_QUERY
    PREFIX_QUERY = search_pb2.PREFIX_QUERY
    BOOL_QUERY = search_pb2.BOOL_QUERY
    CONST_SCORE_QUERY = search_pb2.CONST_SCORE_QUERY
    FUNCTION_SCORE_QUERY = search_pb2.FUNCTION_SCORE_QUERY
    NESTED_QUERY = search_pb2.NESTED_QUERY
    WILDCARD_QUERY = search_pb2.WILDCARD_QUERY
    MATCH_ALL_QUERY = search_pb2.MATCH_ALL_QUERY
    GEO_BOUNDING_BOX_QUERY = search_pb2.GEO_BOUNDING_BOX_QUERY
    GEO_DISTANCE_QUERY = search_pb2.GEO_DISTANCE_QUERY
    GEO_POLYGON_QUERY = search_pb2.GEO_POLYGON_QUERY
    TERMS_QUERY = search_pb2.TERMS_QUERY

class QueryOperator(Enum):
    OR = search_pb2.OR
    AND = search_pb2.AND

class Query(object):

    def __init__(self):
        pass

class MatchQuery(Query):

    def __init__(self, field_name, text, minimum_should_match=None, operator=None):
        self.field_name = field_name
        self.text = text
        self.minimum_should_match = minimum_should_match
        self.operator = operator

class MatchPhraseQuery(Query):

    def __init__(self, field_name, text):
        self.field_name = field_name
        self.text = text

class MatchAllQuery(Query):

    def __init__(self):
        pass

class TermQuery(Query):

    def __init__(self, field_name, column_value):
        self.field_name = field_name
        self.column_value = column_value

class TermsQuery(Query):

    def __init__(self, field_name, column_values):
        self.field_name = field_name
        self.column_values = column_values

class RangeQuery(Query):

    def __init__(self, field_name, range_from=None, range_to=None, include_lower=True, include_upper=False):
        self.field_name = field_name
        self.range_from = range_from
        self.range_to = range_to
        self.include_lower = include_lower
        self.include_upper = include_upper

class PrefixQuery(Query):

    def __init__(self, field_name, prefix):
        self.field_name = field_name
        self.prefix = prefix

class WildcardQuery(Query):

    def __init__(self, field_name, value):
        self.field_name = field_name
        self.value = value

class BoolQuery(Query):

    def __init__(self, must_queries=[], must_not_queries=[],
        filter_queries=[], should_queries=[], minimum_should_match=None):
        self.must_queries = must_queries
        self.must_not_queries = must_not_queries
        self.filter_queries = filter_queries
        self.should_queries = should_queries
        self.minimum_should_match = minimum_should_match

class NestedQuery(Query):

    def __init__(self, path, query, score_mode=ScoreMode.NONE):
        self.path = path
        self.query = query
        self.score_mode = score_mode

class GeoBoundingBoxQuery(Query):

    def __init__(self, field_name, top_left, bottom_right):
        self.field_name = field_name
        self.top_left = top_left
        self.bottom_right = bottom_right

class GeoDistanceQuery(Query):

    def __init__(self, field_name, center_point, distance):
        self.field_name = field_name
        self.center_point = center_point
        self.distance = distance

class GeoPolygonQuery(Query):

    def __init__(self, field_name, points):
        self.field_name = field_name
        self.points = points

class Collapse(object):

    def __init__(self, field_name):
        self.field_name = field_name

class NestedFilter(object):

    def __init__(self, path, query_filter):
        self.path = path
        self.query_filter = query_filter

class FieldValueFactor(object):

    def __init__(self, field_name):
        self.field_name = field_name

class FunctionScoreQuery(Query):

    def __init__(self, query, field_value_factor):
        self.query = query
        self.field_value_factor = field_value_factor

class ExistsQuery(Query):

    def __init__(self, field_name):
        self.field_name = field_name

class SearchQuery(object):

    def __init__(self, query, sort=None, get_total_count=False, 
                 next_token=None, offset=None, limit=None, 
                 aggs = None, group_bys = None):

        self.query = query
        self.sort = sort
        self.get_total_count = get_total_count
        self.next_token = next_token
        self.offset = offset
        self.limit = limit
        self.aggs = aggs
        self.group_bys = group_bys

class ScanQuery(object):

    def __init__(self, query, limit, next_token, current_parallel_id, max_parallel, alive_time = 60):
        self.query = query
        self.limit = limit
        self.next_token = next_token
        self.current_parallel_id = current_parallel_id
        self.max_parallel = max_parallel
        self.alive_time = alive_time

class ColumnReturnType(Enum):

    ALL = search_pb2.RETURN_ALL
    SPECIFIED = search_pb2.RETURN_SPECIFIED
    NONE = search_pb2.RETURN_NONE
    ALL_FROM_INDEX = search_pb2.RETURN_ALL_FROM_INDEX

class ColumnsToGet(object):

    def __init__(self, column_names=[], return_type=ColumnReturnType.NONE):
        self.column_names = column_names
        self.return_type = return_type

class IterableResponse(object):
    def __init__(self):
        self.index = 0
        self.response = tuple()

    def __iter__(self):
        return iter(self.response)

    def _add_response(self, *responses):
        self.response = tuple(responses)

    def v1_response(self):
        return self.response

class SearchResponse(IterableResponse):
    
    def __init__(self, rows, agg_results, group_by_results, next_token, is_all_succeed, total_count):
        super(SearchResponse, self).__init__()

        self.rows = rows
        self.agg_results = agg_results
        self.group_by_results = group_by_results
        self.next_token = next_token
        self.is_all_succeed = is_all_succeed
        self.total_count = total_count

        self._add_response(self.rows, self.next_token, self.total_count, self.is_all_succeed, 
                           self.agg_results, self.group_by_results)        

class ComputeSplitsResponse(IterableResponse):
    
    def __init__(self, session_id, splits_size):
        super(ComputeSplitsResponse, self).__init__()

        self.session_id = session_id
        self.splits_size = splits_size

        self._add_response(self.session_id, self.splits_size)

class ParallelScanResponse(IterableResponse):
    
    def __init__(self, rows, next_token):
        super(ParallelScanResponse, self).__init__()

        self.rows = rows
        self.next_token = next_token

        self._add_response(self.rows, self.next_token)
