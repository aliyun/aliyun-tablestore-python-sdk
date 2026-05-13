# -*- coding: utf8 -*-

import six
from enum import IntEnum
from typing import List
from tablestore.error import *
from tablestore.utils import DefaultJsonObject
from tablestore.protobuf import search_pb2


class TableMeta(DefaultJsonObject):

    def __init__(self, table_name, schema_of_primary_key, defined_columns=None):
        # schema_of_primary_key: [('PK0', 'STRING'), ('PK1', 'INTEGER'), ...]
        self.table_name = table_name
        self.schema_of_primary_key = schema_of_primary_key
        self.defined_columns = [] if defined_columns is None else defined_columns[:]


class TableOptions(DefaultJsonObject):
    def __init__(self, time_to_live=-1, max_version=1, max_time_deviation=86400, allow_update=None, update_full_row=None):
        self.time_to_live = time_to_live
        self.max_version = max_version
        self.max_time_deviation = max_time_deviation
        self.allow_update = allow_update
        self.update_full_row = update_full_row


class SSEKeyType(object):
    SSE_KMS_SERVICE = 1
    SSE_BYOK = 2


class SSESpecification(DefaultJsonObject):
    def __init__(self, enable=False, key_type=None, key_id=None, role_arn=None):
        self.enable = enable
        self.key_type = key_type
        self.key_id = key_id
        self.role_arn = role_arn
        self.check_arguments()

    def check_arguments(self):
        if self.enable:
            if self.key_type is None:
                raise OTSClientError("key type is required when enable is true")
            else:
                if self.key_type != SSEKeyType.SSE_BYOK and \
                        (self.key_id is not None or self.role_arn is not None):
                    raise OTSClientError("key id and role arn cannot be set when key type is not SSE_BYOK")
                if self.key_type != SSEKeyType.SSE_KMS_SERVICE and \
                        (self.key_id is None or self.role_arn is None):
                    raise OTSClientError("key id and role arn are required when key type is not SSE_KMS_SERVICE")
        else:
            if self.key_type is not None:
                raise OTSClientError("key type cannot be set when enable is false")


class SSEDetails(DefaultJsonObject):
    def __init__(self, enable=False, key_type=None, key_id=None, role_arn=None):
        self.enable = enable
        self.key_type = key_type
        self.key_id = key_id
        self.role_arn = role_arn

class CapacityUnit(DefaultJsonObject):

    def __init__(self, read=0, write=0):
        self.read = read
        self.write = write


class ReservedThroughput(DefaultJsonObject):

    def __init__(self, capacity_unit):
        self.capacity_unit = capacity_unit


class ReservedThroughputDetails(DefaultJsonObject):

    def __init__(self, capacity_unit, last_increase_time, last_decrease_time):
        self.capacity_unit = capacity_unit
        self.last_increase_time = last_increase_time
        self.last_decrease_time = last_decrease_time


class FieldType(IntEnum):
    LONG = search_pb2.LONG
    DOUBLE = search_pb2.DOUBLE
    BOOLEAN = search_pb2.BOOLEAN
    KEYWORD = search_pb2.KEYWORD
    TEXT = search_pb2.TEXT
    NESTED = search_pb2.NESTED
    GEOPOINT = search_pb2.GEO_POINT
    DATE = search_pb2.DATE
    VECTOR = search_pb2.VECTOR
    JSON = search_pb2.JSON


class AnalyzerType(object):
    SINGLEWORD = "single_word"
    MAXWORD = "max_word"
    MINWORD = "min_word"
    FUZZY = "fuzzy"
    SPLIT = "split"


class SingleWordAnalyzerParameter(DefaultJsonObject):
    def __init__(self, case_sensitive=False, delimit_word=False):
        self.case_sensitive = case_sensitive
        self.delimit_word = delimit_word


class SplitAnalyzerParameter(DefaultJsonObject):
    def __init__(self, delimiter=' '):
        self.delimiter = delimiter


class FuzzyAnalyzerParameter(DefaultJsonObject):
    def __init__(self, min_chars=1, max_chars=7):
        self.min_chars = min_chars
        self.max_chars = max_chars


class ScoreMode(IntEnum):
    NONE = search_pb2.SCORE_MODE_NONE
    AVG = search_pb2.SCORE_MODE_AVG
    MAX = search_pb2.SCORE_MODE_MAX
    TOTAL = search_pb2.SCORE_MODE_TOTAL
    MIN = search_pb2.SCORE_MODE_MIN


class SortMode(IntEnum):
    MIN = search_pb2.SORT_MODE_MIN
    MAX = search_pb2.SORT_MODE_MAX
    AVG = search_pb2.SORT_MODE_AVG


class SortOrder(IntEnum):
    ASC = search_pb2.SORT_ORDER_ASC
    DESC = search_pb2.SORT_ORDER_DESC


class GeoDistanceType(IntEnum):
    ARC = search_pb2.GEO_DISTANCE_ARC
    PLANE = search_pb2.GEO_DISTANCE_PLANE


class Sorter(DefaultJsonObject):
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


class DocSort(Sorter):

    def __init__(self, sort_order=SortOrder.ASC):
        self.sort_order = sort_order


class Sort(DefaultJsonObject):

    def __init__(self, sorters):
        self.sorters = sorters


class IndexSetting(DefaultJsonObject):

    def __init__(self, routing_fields=None):
        self.routing_fields = [] if routing_fields is None else routing_fields[:]


class VectorDataType(IntEnum):
    VD_FLOAT_32 = search_pb2.VD_FLOAT_32


class VectorMetricType(IntEnum):
    VM_EUCLIDEAN = search_pb2.VM_EUCLIDEAN
    VM_COSINE = search_pb2.VM_COSINE
    VM_DOT_PRODUCT = search_pb2.VM_DOT_PRODUCT


class VectorOptions(DefaultJsonObject):

    def __init__(self, data_type, metric_type, dimension):
        self.data_type = data_type
        self.metric_type = metric_type
        self.dimension = dimension


class JsonType(IntEnum):
    OBJECT_JSON = search_pb2.OBJECT_JSON
    NESTED_JSON = search_pb2.NESTED_JSON


class TextSimilarity(IntEnum):
    BM25 = search_pb2.BM25
    SHORT_TEXT = search_pb2.SHORT_TEXT


class FieldSchema(DefaultJsonObject):

    def __init__(self, field_name, field_type, index=None,
                 store=None, is_array=None, enable_sort_and_agg=None,
                 analyzer=None, sub_field_schemas=None, analyzer_parameter=None,
                 date_formats=None, is_virtual_field=False, source_fields=None, vector_options=None,
                 enable_highlighting=None, json_type=None, text_similarity=None):
        self.field_name = field_name
        self.field_type = field_type
        self.index = index
        self.store = store
        self.is_array = is_array
        self.enable_sort_and_agg = enable_sort_and_agg
        self.analyzer = analyzer
        self.analyzer_parameter = analyzer_parameter
        self.sub_field_schemas = [] if sub_field_schemas is None else sub_field_schemas[:]
        self.date_formats = [] if date_formats is None else date_formats[:]
        self.is_virtual_field = is_virtual_field
        self.source_fields = [] if source_fields is None else source_fields[:]
        self.vector_options = vector_options
        self.enable_highlighting = enable_highlighting
        self.json_type = json_type
        self.text_similarity = text_similarity


class SyncPhase(IntEnum):
    FULL = 0
    INCR = 1


class SyncStat(DefaultJsonObject):

    def __init__(self, sync_phase, current_sync_timestamp):
        self.sync_phase = sync_phase
        self.current_sync_timestamp = current_sync_timestamp


class SearchIndexMeta(DefaultJsonObject):

    def __init__(self, fields, index_setting=None, index_sort=None, time_to_live=-1):
        self.fields = fields
        self.index_setting = index_setting
        self.index_sort = index_sort
        self.time_to_live = time_to_live


class DefinedColumnSchema(DefaultJsonObject):

    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type


class SecondaryIndexType(IntEnum):
    GLOBAL_INDEX = 0
    LOCAL_INDEX = 1


class SyncType(IntEnum):
    SYNC_TYPE_FULL = 1
    SYNC_TYPE_INCR = 2


class SecondaryIndexMeta(DefaultJsonObject):

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


class Column(DefaultJsonObject):
    def __init__(self, name, value=None, timestamp=None):
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


class CommonResponse(DefaultJsonObject):
    def __init__(self):
        self.request_id = ''

    def set_request_id(self, request_id):
        self.request_id = request_id


class UpdateTableResponse(CommonResponse):

    def __init__(self, reserved_throughput_details, table_options):
        self.reserved_throughput_details = reserved_throughput_details
        self.table_options = table_options


class DescribeTableResponse(CommonResponse):

    def __init__(self, table_meta, table_options, reserved_throughput_details, secondary_indexes=None, sse_details=None):
        self.table_meta = table_meta
        self.table_options = table_options
        self.reserved_throughput_details = reserved_throughput_details
        self.sse_details = sse_details
        self.secondary_indexes = [] if secondary_indexes is None else secondary_indexes[:]


class RowDataItem(DefaultJsonObject):

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

class CastType(IntEnum):
    VT_INTEGER = 0
    VT_DOUBLE = 1
    VT_STRING = 2

class ComparatorType(object):
    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER_EQUAL = 3
    LESS_THAN = 4
    LESS_EQUAL = 5
    EXIST = 6
    NOT_EXIST = 7

    __values__ = [
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_EQUAL,
        LESS_THAN,
        LESS_EQUAL,
        EXIST,
        NOT_EXIST,
    ]

    __single_condition_values__ = [
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
        "ComparatorType.EXIST",
        "ComparatorType.NOT_EXIST",
    ]

    __single_condition_members__ = [
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
    SINGLE_COLUMN_REGEX_CONDITION = 2


class ColumnCondition(DefaultJsonObject):
    pass


class CompositeColumnCondition(ColumnCondition):

    def __init__(self, combinator):
        self.sub_conditions = []
        if combinator not in LogicalOperator.__values__:
            raise OTSClientError(
                "Expect input combinator should be one of %s, but '%s'" % (str(LogicalOperator.__members__), combinator)
            )
        self.combinator = combinator

    def get_type(self):
        return ColumnConditionType.COMPOSITE_COLUMN_CONDITION

    def get_combinator(self):
        return self.combinator

    def add_sub_condition(self, condition):
        if not isinstance(condition, ColumnCondition):
            raise OTSClientError(
                "The input condition should be an instance of ColumnCondition, not %s" %
                condition.__class__.__name__
            )

        self.sub_conditions.append(condition)

    def clear_sub_condition(self):
        self.sub_conditions = []


class SingleColumnCondition(ColumnCondition):

    def __init__(self, column_name, column_value, comparator, pass_if_missing=True, latest_version_only=True):
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
        Set ```pass_if_missing```

        Since the attribute columns of a row in OTS are not fixed, there might be cases where the column with a condition does not exist in that row. In such cases, 
        this parameter controls the check result for that row.
        If set to True, the check will pass if the column does not exist in the row.
        If set to False, the check will fail if the column does not exist in the row.
        The default value is True.
        """
        if not isinstance(pass_if_missing, bool):
            raise OTSClientError(
                "The input pass_if_missing of SingleColumnCondition should be an instance of Bool, not %s" %
                pass_if_missing.__class__.__name__
            )
        self.pass_if_missing = pass_if_missing

    def get_pass_if_missing(self):
        return self.pass_if_missing

    def set_latest_version_only(self, latest_version_only):
        if not isinstance(latest_version_only, bool):
            raise OTSClientError(
                "The input latest_version_only of SingleColumnCondition should be an instance of Bool, not %s" %
                latest_version_only.__class__.__name__
            )
        self.latest_version_only = latest_version_only

    def get_latest_version_only(self):
        return self.latest_version_only

    def set_column_name(self, column_name):
        if not isinstance(column_name, (six.text_type, six.binary_type)):
            raise OTSClientError(
                "The input column_name of SingleColumnCondition should be an instance of str, not %s" %
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
        if comparator not in ComparatorType.__single_condition_values__:
            raise OTSClientError(
                "Expect input comparator of SingleColumnCondition should be one of %s, but '%s'" %
                (str(ComparatorType.__single_condition_members__), comparator)
            )
        self.comparator = comparator

    def get_comparator(self):
        return self.comparator

class RegexRule(DefaultJsonObject):
    def __init__(self, regex_input: str, cast_type: CastType):
        self.regex = None
        self.cast_type = None
        self.set_regex(regex_input)
        self.set_cast_type(cast_type)

    def set_regex(self, regex_input):
        if not isinstance(regex_input, (six.text_type, six.binary_type)):
            raise OTSClientError("regex_input should be an instance of str, not %s" %
                                 regex_input.__class__.__name__)
        self.regex = regex_input

    def set_cast_type(self, cast_type):
        if not isinstance(cast_type, CastType):
            raise OTSClientError("input cast_type should be an instance of CastType, not %s" %
                                 cast_type.__class__.__name__)
        self.cast_type = cast_type

    def get_regex(self):
        return self.regex

    def get_cast_type(self):
        return self.cast_type

class SingleColumnRegexCondition(ColumnCondition):
    def __init__(self, column_name, comparator,
                 column_value=None, regex_rule: RegexRule = None, latest_version_only=True):
        self.column_name = None
        self.comparator = None
        self.column_value = None
        self.latest_version_only = None
        self.regex_rule = None

        self.set_column_name(column_name)
        self.set_comparator(comparator)
        self.check_arguments(column_value, comparator)
        if column_value is None:
            column_value = 0
        self.set_column_value(column_value)
        self.set_regex_rule(regex_rule)
        self.set_latest_version_only(latest_version_only)

    def get_type(self):
        return ColumnConditionType.SINGLE_COLUMN_REGEX_CONDITION

    @staticmethod
    def check_arguments(column_value, comparator):
        if column_value is not None and comparator in [ComparatorType.EXIST, ComparatorType.NOT_EXIST]:
            raise OTSClientError(
                "when column_value is set, comparator should not be EXIST or NOT_EXIST, but now is %s" % ComparatorType.__members__[comparator])
        if column_value is None and comparator not in [ComparatorType.EXIST, ComparatorType.NOT_EXIST]:
            raise OTSClientError(
                "when column_value is not set, comparator should be EXIST or NOT_EXIST, but now is %s" % ComparatorType.__members__[comparator])

    def set_column_name(self, column_name):
        if not isinstance(column_name, (six.text_type, six.binary_type)):
            raise OTSClientError(
                "The input column_name of SingleColumnRegexCondition should be an instance of str, not %s" %
                column_name.__class__.__name__
            )
        if column_name is None:
            raise OTSClientError("The input column_name of SingleColumnRegexCondition should not be None")
        self.column_name = column_name

    def get_column_name(self):
        return self.column_name

    def set_comparator(self, comparator):
        if comparator not in ComparatorType.__values__:
            raise OTSClientError(
                "Expect input comparator of SingleColumnRegexCondition should be one of %s, but '%s'" %
                (str(ComparatorType.__members__), comparator)
            )
        self.comparator = comparator

    def get_comparator(self):
        return self.comparator

    def set_column_value(self, column_value):
        if column_value is None:
            raise OTSClientError(
                "The input column_value of SingleColumnRegexCondition.set_column_value should not be None")
        self.column_value = column_value

    def get_column_value(self):
        return self.column_value

    def set_regex_rule(self, regex_rule):
        if regex_rule is not None and not isinstance(regex_rule, RegexRule):
            raise OTSClientError("input regex_rule should be an instance of RegexRule or None, not %s" %
                                 regex_rule.__class__.__name__)
        self.regex_rule = regex_rule

    def get_regex_rule(self):
        return self.regex_rule

    def set_latest_version_only(self, latest_version_only):
        if not isinstance(latest_version_only, bool):
            raise OTSClientError(
                "The input latest_version_only of SingleColumnRegexCondition should be an instance of Bool, not %s" %
                latest_version_only.__class__.__name__
            )
        self.latest_version_only = latest_version_only

    def get_latest_version_only(self):
        return self.latest_version_only

    @staticmethod
    def get_pass_if_missing():
        """Always false for SingleColumnRegexCondition"""
        return False

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


class Condition(DefaultJsonObject):

    def __init__(self, row_existence_expectation, column_condition=None):
        self.row_existence_expectation = None
        self.column_condition = None

        self.set_row_existence_expectation(row_existence_expectation)
        if column_condition != None:
            self.set_column_condition(column_condition)

    def set_row_existence_expectation(self, row_existence_expectation):
        if row_existence_expectation not in RowExistenceExpectation.__values__:
            raise OTSClientError(
                "Expect input row_existence_expectation should be one of %s, but '%s'" % (
                    str(RowExistenceExpectation.__members__), row_existence_expectation)
            )

        self.row_existence_expectation = row_existence_expectation

    def get_row_existence_expectation(self):
        return self.row_existence_expectation

    def set_column_condition(self, column_condition):
        if not isinstance(column_condition, ColumnCondition):
            raise OTSClientError(
                "The input column_condition should be an instance of ColumnCondition, not %s" %
                column_condition.__class__.__name__
            )
        self.column_condition = column_condition

    def get_column_condition(self):
        return self.column_condition


class Row(DefaultJsonObject):
    def __init__(self, primary_key, attribute_columns=None):
        self.primary_key = primary_key
        self.attribute_columns = attribute_columns


class RowItem(DefaultJsonObject):

    def __init__(self, row_type, row, condition, return_type=None):
        self.type = row_type
        self.condition = condition
        self.row = row
        self.return_type = return_type


class PutRowItem(RowItem):

    def __init__(self, row, condition, return_type=None):
        super(PutRowItem, self).__init__(BatchWriteRowType.PUT, row, condition, return_type)


class UpdateRowItem(RowItem):

    def __init__(self, row, condition, return_type=None):
        super(UpdateRowItem, self).__init__(BatchWriteRowType.UPDATE, row, condition, return_type)


class DeleteRowItem(RowItem):

    def __init__(self, row, condition, return_type=None):
        super(DeleteRowItem, self).__init__(BatchWriteRowType.DELETE, row, condition, return_type)


class TableInBatchGetRowItem(DefaultJsonObject):

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


class BatchGetRowRequest(DefaultJsonObject):

    def __init__(self):
        self.items = {}

    def add(self, table_item):
        """
        Description: Add a tablestore.metadata.TableInBatchGetRowItem object.
        Note: The internal storage of the object uses a 'dictionary' format, where the key is the table 
              name. Therefore, if an object with the same table name is inserted, the previous object will 
              be overwritten.
        """
        if not isinstance(table_item, TableInBatchGetRowItem):
            raise OTSClientError(
                "The input table_item should be an instance of TableInBatchGetRowItem, not %s" %
                table_item.__class__.__name__
            )

        self.items[table_item.table_name] = table_item


class BatchGetRowResponse(DefaultJsonObject):

    def __init__(self, table_rows):
        self.items = table_rows

    def get_failed_rows(self):
        return [row for rows in self.items.values() for row in rows if not row.is_ok]

    def get_succeed_rows(self):
        return [row for rows in self.items.values() for row in rows if row.is_ok]

    def get_result(self):
        succ = self.get_succeed_rows()
        fail = self.get_failed_rows()

        return succ, fail

    def get_result_by_table(self, table_name):
        return self.items.get(table_name)

    def is_all_succeed(self):
        return self.get_failed_rows() == []


class BatchWriteRowType(object):
    PUT = "put"
    UPDATE = "update"
    DELETE = "delete"


class TableInBatchWriteRowItem(DefaultJsonObject):

    def __init__(self, table_name, row_items):
        self.table_name = table_name
        self.row_items = row_items


class BatchWriteRowRequest(DefaultJsonObject):

    def __init__(self):
        self.items = {}
        self.transaction_id = None

    def add(self, table_item):
        """
        Description: Add a tablestore.metadata.TableInBatchWriteRowItem object.
        Note: The internal storage of the tablestore.metadata.TableInBatchWriteRowItem object uses a 'dictionary' format, 
              where the key is the table name. Therefore, if an object with the same table name is inserted, 
              the previous object will be overwritten.
        """
        if not isinstance(table_item, TableInBatchWriteRowItem):
            raise OTSClientError(
                "The input table_item should be an instance of TableInBatchWriteRowItem, not %s" %
                table_item.__class__.__name__
            )

        self.items[table_item.table_name] = table_item

    def set_transaction_id(self, transcation_id):
        self.transaction_id = transcation_id


class BatchWriteRowResponse(DefaultJsonObject):

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
                row_item.set_index(index)
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
        return self.get_failed_of_put() == [] and self.get_failed_of_update() == [] and self.get_failed_of_delete() == []


class BatchWriteRowResponseItem(DefaultJsonObject):

    def __init__(self, is_ok, error_code, error_message, consumed, primary_key):
        self.is_ok = is_ok
        self.error_code = error_code
        self.error_message = error_message
        self.consumed = consumed
        self.row = Row(primary_key)

    def set_index(self, index):
        self.index = index


class INF_MIN(object):
    # for get_range
    pass


class INF_MAX(object):
    # for get_range
    pass


class PK_AUTO_INCR(object):
    # for put_row
    pass


class QueryType(IntEnum):
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
    KNN_VECTOR_QUERY = search_pb2.KNN_VECTOR_QUERY
    DIS_MAX_QUERY = search_pb2.DIS_MAX_QUERY


class QueryOperator(IntEnum):
    OR = search_pb2.OR
    AND = search_pb2.AND


class Query(DefaultJsonObject):

    def __init__(self):
        pass


class MatchQuery(Query):

    def __init__(self, field_name, text, minimum_should_match=None, operator=None, weight=None):
        self.field_name = field_name
        self.text = text
        self.minimum_should_match = minimum_should_match
        self.operator = operator
        self.weight = weight


class MatchPhraseQuery(Query):

    def __init__(self, field_name, text, weight=None, slop=None):
        self.field_name = field_name
        self.text = text
        self.weight = weight
        self.slop = slop

class MatchAllQuery(Query):

    def __init__(self):
        pass


class TermQuery(Query):

    def __init__(self, field_name, column_value, weight=None):
        self.field_name = field_name
        self.column_value = column_value
        self.weight = weight


class TermsQuery(Query):

    def __init__(self, field_name, column_values, weight=None):
        self.field_name = field_name
        self.column_values = column_values
        self.weight = weight


class RangeQuery(Query):

    def __init__(self, field_name, range_from=None, range_to=None, include_lower=True, include_upper=False):
        self.field_name = field_name
        self.range_from = range_from
        self.range_to = range_to
        self.include_lower = include_lower
        self.include_upper = include_upper


class PrefixQuery(Query):

    def __init__(self, field_name, prefix, weight=None):
        self.field_name = field_name
        self.prefix = prefix
        self.weight = weight


class WildcardQuery(Query):

    def __init__(self, field_name, value, weight=None):
        self.field_name = field_name
        self.value = value
        self.weight = weight


class BoolQuery(Query):

    def __init__(self, must_queries=None, must_not_queries=None,
                 filter_queries=None, should_queries=None, minimum_should_match=None, weight=None):
        self.must_queries = [] if must_queries is None else must_queries[:]
        self.must_not_queries = [] if must_not_queries is None else must_not_queries[:]
        self.filter_queries = [] if filter_queries is None else filter_queries[:]
        self.should_queries = [] if should_queries is None else should_queries[:]
        self.minimum_should_match = minimum_should_match
        self.weight = weight


class NestedQuery(Query):

    def __init__(self, path, query, score_mode=ScoreMode.NONE, inner_hits=None, weight=None):
        self.path = path
        self.query = query
        self.score_mode = score_mode
        self.inner_hits = inner_hits
        self.weight = weight


class InnerHits(object):
    def __init__(self, sort, offset, limit, highlight):
        self.sort = sort
        self.offset = offset
        self.limit = limit
        self.highlight = highlight


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


class Collapse(DefaultJsonObject):

    def __init__(self, field_name):
        self.field_name = field_name


class NestedFilter(DefaultJsonObject):

    def __init__(self, path, query_filter):
        self.path = path
        self.query_filter = query_filter


class FieldValueFactor(DefaultJsonObject):

    def __init__(self, field_name):
        self.field_name = field_name


class FunctionScoreQuery(Query):

    def __init__(self, query, field_value_factor):
        self.query = query
        self.field_value_factor = field_value_factor


class ExistsQuery(Query):

    def __init__(self, field_name):
        self.field_name = field_name


class KnnVectorQuery(Query):

    def __init__(self, field_name, top_k=None, float32_query_vector=None, filter=None, weight=None, min_score=None,
                 num_candidates=None):
        self.field_name = field_name
        self.top_k = top_k
        self.float32_query_vector = float32_query_vector
        self.filter = filter
        self.weight = weight
        self.min_score = min_score
        self.num_candidates = num_candidates


class DisMaxQuery(Query):

    def __init__(self, queries=None, tie_breaker=None, weight=None):
        self.queries = [] if queries is None else queries
        self.tie_breaker = tie_breaker
        self.weight = weight


class SearchQuery(DefaultJsonObject):

    def __init__(self, query, sort=None, get_total_count=False,
                 next_token=None, offset=None, limit=None,
                 aggs=None, group_bys=None, collapse_field=None,
                 highlight=None):
        self.query = query
        self.sort = sort
        self.get_total_count = get_total_count
        self.next_token = next_token
        self.offset = offset
        self.limit = limit
        self.aggs = aggs
        self.group_bys = group_bys
        self.collapse = collapse_field
        self.highlight = highlight


class ScanQuery(DefaultJsonObject):

    def __init__(self, query, limit, next_token, current_parallel_id, max_parallel, alive_time=60):
        self.query = query
        self.limit = limit
        self.next_token = next_token
        self.current_parallel_id = current_parallel_id
        self.max_parallel = max_parallel
        self.alive_time = alive_time


class HighlightFragmentOrder(IntEnum):
    TEXT_SEQUENCE = search_pb2.TEXT_SEQUENCE
    SCORE = search_pb2.SCORE


class HighlightEncoder(IntEnum):
    PLAIN_MODE = search_pb2.PLAIN_MODE
    HTML_MODE = search_pb2.HTML_MODE


class HighlightParameter(object):

    def __init__(self, field_name, number_of_fragments=None, fragment_size=None,
                 pre_tag=None, post_tag=None,
                 fragments_order=HighlightFragmentOrder.TEXT_SEQUENCE):
        self.field_name = field_name
        self.number_of_fragments = number_of_fragments
        self.fragment_size = fragment_size
        self.pre_tag = pre_tag
        self.post_tag = post_tag
        self.fragments_order = fragments_order


class Highlight(object):

    def __init__(self, highlight_parameters, highlight_encoder=HighlightEncoder.PLAIN_MODE):
        self.highlight_parameters = highlight_parameters
        self.highlight_encoder = highlight_encoder


class ColumnReturnType(IntEnum):
    ALL = search_pb2.RETURN_ALL
    SPECIFIED = search_pb2.RETURN_SPECIFIED
    NONE = search_pb2.RETURN_NONE
    ALL_FROM_INDEX = search_pb2.RETURN_ALL_FROM_INDEX


class ColumnsToGet(DefaultJsonObject):

    def __init__(self, column_names=None, return_type=ColumnReturnType.NONE):
        self.column_names = [] if column_names is None else column_names[:]
        self.return_type = return_type


class IterableResponse(CommonResponse):
    def __init__(self):
        super(IterableResponse, self).__init__()

        self.index = 0
        self.response = tuple()

    def __iter__(self):
        return iter(self.response)

    def _add_response(self, *responses):
        self.response = tuple(responses)

    def v1_response(self):
        return self.response


class SearchResponse(IterableResponse):

    def __init__(self, rows, agg_results, group_by_results, next_token, is_all_succeed, total_count, search_hits):
        super(SearchResponse, self).__init__()

        self.rows = rows
        self.agg_results = agg_results
        self.group_by_results = group_by_results
        self.next_token = next_token
        self.is_all_succeed = is_all_succeed
        self.total_count = total_count
        self.search_hits = search_hits

        self._add_response(self.rows, self.next_token, self.total_count, self.is_all_succeed,
                           self.agg_results, self.group_by_results, self.search_hits)


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


'''
    Search Hit
'''


class SearchHit(object):
    def __init__(self, row, score, highlight_result, search_inner_hits, nested_doc_offset):
        self.row = row
        self.score = score
        self.highlight_result = highlight_result
        self.search_inner_hits = search_inner_hits
        self.nested_doc_offset = nested_doc_offset


class SearchInnerHit(object):
    def __init__(self, path, search_hits):
        self.path = path
        self.search_hits = search_hits


class HighlightResult(object):
    def __init__(self, highlight_fields):
        self.highlight_fields = highlight_fields


class HighlightField(object):
    def __init__(self, field_name, field_fragments):
        self.field_name = field_name
        self.field_fragments = field_fragments


class TimeseriesKey(object):
    def __init__(self, measurement_name: str = None, data_source: str = None, tags = None):
        self.measurement_name = measurement_name
        self.data_source = data_source
        self.tags = tags


class TimeseriesRow(object):
    def __init__(self, timeseries_key: TimeseriesKey, fields, time_in_us: int):
        self.timeseries_key = timeseries_key
        self.fields = fields
        self.time_in_us = time_in_us


class TimeseriesTableOptions(object):
    def __init__(self, time_to_live: int):
        self.time_to_live = time_to_live


class TimeseriesMetaOptions(object):
    def __init__(self, meta_time_to_live: int = None, allow_update_attributes: bool = None):
        self.meta_time_to_live = meta_time_to_live
        self.allow_update_attributes = allow_update_attributes


class TimeseriesTableMeta(object):
    """
        Table structure information, including the table name and configuration details.

        Parameters:
            timeseries_table_name Name of the table.
            timeseries_table_options Table configuration options, including data TTL, etc.
            timeseries_meta_options Timeline metadata-related configuration options, including metadata TTL, etc.
            status The status of the table.
            timeseries_keys Custom primary key columns.
            field_primary_keys Extended primary key columns. Example value: `[('gid', 'INTEGER'), ('uid', 'INTEGER', PK_AUTO_INCR)]`
        """

    def __init__(
            self,
            timeseries_table_name: str,
            timeseries_table_options: TimeseriesTableOptions = None,
            timeseries_meta_options: TimeseriesMetaOptions = None,
            timeseries_keys: List[str] = None,
            field_primary_keys=None
    ):
        self.timeseries_table_name = timeseries_table_name
        self.timeseries_table_options = timeseries_table_options
        self.timeseries_meta_options = timeseries_meta_options
        self.status = None
        self.timeseries_keys = timeseries_keys
        self.field_primary_keys = field_primary_keys


class TimeseriesAnalyticalStore(object):
    """
    Analyze and store configuration information
    Parameters:
    `analytical_store_name` The name of the analytical store
    `time_to_live` Data retention time for the analytical store
    `sync_option` Synchronization method for the analytical store, optional values: `SYNC_TYPE_FULL` `SYNC_TYPE_INCR`
    """

    def __init__(self, analytical_store_name: str, time_to_live: int = None, sync_option=None):
        self.analytical_store_name = analytical_store_name
        self.time_to_live = time_to_live
        self.sync_option = sync_option


class LastpointIndexMeta(object):
    """
    Lastpoint index configuration information
    Parameters:
    index_table_name Lastpoint index name
    """

    def __init__(self, index_table_name: str):
        self.index_table_name = index_table_name


class CreateTimeseriesTableRequest(object):

    """
    Create a time-series table request
    Parameters:
    table_meta Configuration information for the time-series table
    analytical_stores Configuration information for creating analytical storage
    enable_analytical_store Whether to enable analytical storage
    lastpoint_index_metas Lastpoint index configuration
"""
    def __init__(self,
                 table_meta: TimeseriesTableMeta,
                 analytical_stores: List[TimeseriesAnalyticalStore] = None,
                 lastpoint_index_metas: List[LastpointIndexMeta] = None):
        self.table_meta = table_meta
        self.analytical_stores = analytical_stores
        self.lastpoint_index_metas = lastpoint_index_metas


class DescribeTimeseriesTableResponse(object):

    def __init__(self, table_meta: TimeseriesTableMeta):
        self.table_meta = table_meta


class UpdateTimeseriesMetaRequest(object):

    def __init__(self, timeseries_tablename: str, metas: list):
        self.timeseries_tablename = timeseries_tablename
        self.metas = metas


class DeleteTimeseriesMetaRequest(object):

    def __init__(self, timeseries_tablename: str, keys: list):
        self.timeseries_tablename = timeseries_tablename
        self.timeseries_keys = keys


class TimeseriesMeta(object):

    def __init__(self, timeseriesKey: TimeseriesKey, attributes, updateTimeInUs: int = None):
        self.timeseries_key = timeseriesKey
        self.attributes = attributes
        self.update_time_in_us = updateTimeInUs


class Error(object):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message

class FailedRowResult(object):
    def __init__(self, index, error: Error):
        self.index = index
        self.error = error

class UpdateTimeseriesMetaResponse(object):
    def __init__(self, failed_rows):
        self.failedRows = failed_rows

class PutTimeseriesDataResponse(object):
    def __init__(self, failed_rows:FailedRowResult = None):
        self.failedRows = failed_rows


class DeleteTimeseriesMetaResponse(object):
    def __init__(self, failed_rows):
        self.failedRows = failed_rows


class QueryTimeseriesMetaRequest(object):

    def __init__(self, timeseriesTableName: str, condition=None, getTotalHits=False, limit=None, nextToken=None):
        self.timeseriesTableName = timeseriesTableName
        self.condition = condition
        self.getTotalHits = getTotalHits
        self.limit = limit
        self.nextToken = nextToken


class QueryTimeseriesMetaResponse(object):
    def __init__(self, timeseriesMetas: TimeseriesMeta=None, totalHits=None, nextToken=None):
        self.timeseriesMetas = timeseriesMetas
        self.totalHits = totalHits
        self.nextToken = nextToken


class GetTimeseriesDataRequest(object):

    def __init__(self, timeseriesTableName, timeseriesKey = None, beginTimeInUs=0, endTimeInUs=0, limit=0, nextToken=None, backward=False, fieldsToGet=None):
        self.timeseriesTableName = timeseriesTableName
        self.timeseriesKey = timeseriesKey
        self.beginTimeInUs = beginTimeInUs
        self.endTimeInUs = endTimeInUs
        self.limit = limit
        self.nextToken = nextToken
        self.backward = backward
        self.fieldsToGet = fieldsToGet


class GetTimeseriesDataResponse(object):

    def __init__(self, rows: TimeseriesRow = None, nextToken = None):
        self.rows = rows
        self.nextToken = nextToken


# ==================== Global Table ====================

class SyncMode(object):
    ROW = 'row'
    COLUMN = 'column'

class ServeMode(object):
    PRIMARY_SECONDARY = 'primary_secondary'
    PEER_TO_PEER = 'peer_to_peer'

class GlobalTableStatus(object):
    INIT = 'init'
    RECONF = 'reconf'
    ACTIVE = 'active'

class PhyTableStatus(object):
    PENDING = 'pending'
    INIT = 'init'
    SYNCDATA = 'syncdata'
    READY = 'ready'
    ACTIVE = 'active'
    UNBINDING = 'unbinding'
    UNBOUND = 'unbound'

class PhyTableSyncStage(object):
    INIT = 'init'
    FULL = 'full'
    INCR = 'incr'

class BaseTable(DefaultJsonObject):
    def __init__(self, region_id, instance_name, table_name):
        self.region_id = region_id
        self.instance_name = instance_name
        self.table_name = table_name

class Placement(DefaultJsonObject):
    def __init__(self, region_id, instance_name, writable=False):
        self.region_id = region_id
        self.instance_name = instance_name
        self.writable = writable

class Removal(DefaultJsonObject):
    def __init__(self, region_id, instance_name):
        self.region_id = region_id
        self.instance_name = instance_name

class PhyTable(DefaultJsonObject):
    def __init__(self, region_id, instance_name, table_name, writable=False,
                 status=None, status_timestamp=None, table_id=None,
                 stage=None, is_failed=False, message=None, role=None, rpo_nanos=None):
        self.region_id = region_id
        self.instance_name = instance_name
        self.table_name = table_name
        self.writable = writable
        self.status = status
        self.status_timestamp = status_timestamp
        self.table_id = table_id
        self.stage = stage
        self.is_failed = is_failed
        self.message = message
        self.role = role
        self.rpo_nanos = rpo_nanos

class UpdatePhyTable(DefaultJsonObject):
    def __init__(self, region_id, instance_name, table_name,
                 writable=None, primary_eligible=None):
        self.region_id = region_id
        self.instance_name = instance_name
        self.table_name = table_name
        self.writable = writable
        self.primary_eligible = primary_eligible

class CreateGlobalTableRequest(DefaultJsonObject):
    def __init__(self, base_table, sync_mode, serve_mode, placements=None):
        self.base_table = base_table
        self.sync_mode = sync_mode
        self.serve_mode = serve_mode
        self.placements = placements if placements is not None else []

class CreateGlobalTableResponse(DefaultJsonObject):
    def __init__(self, global_table_id, request_id=None):
        self.global_table_id = global_table_id
        self.request_id = request_id

class BindGlobalTableRequest(DefaultJsonObject):
    def __init__(self, global_table_id, global_table_name, placements):
        self.global_table_id = global_table_id
        self.global_table_name = global_table_name
        self.placements = placements

class BindGlobalTableResponse(DefaultJsonObject):
    def __init__(self, request_id=None):
        self.request_id = request_id

class UnbindGlobalTableRequest(DefaultJsonObject):
    def __init__(self, global_table_id, global_table_name, removals):
        self.global_table_id = global_table_id
        self.global_table_name = global_table_name
        self.removals = removals

class UnbindGlobalTableResponse(DefaultJsonObject):
    def __init__(self, request_id=None):
        self.request_id = request_id

class DescribeGlobalTableRequest(DefaultJsonObject):
    def __init__(self, global_table_name, global_table_id=None,
                 phy_table=None, return_rpo=None):
        self.global_table_name = global_table_name
        self.global_table_id = global_table_id
        self.phy_table = phy_table
        self.return_rpo = return_rpo

class DescribeGlobalTableResponse(DefaultJsonObject):
    def __init__(self, global_table_id, status, phy_tables, serve_mode,
                 request_id=None):
        self.global_table_id = global_table_id
        self.status = status
        self.phy_tables = phy_tables
        self.serve_mode = serve_mode
        self.request_id = request_id

class UpdateGlobalTableRequest(DefaultJsonObject):
    def __init__(self, global_table_id, global_table_name, phy_table):
        self.global_table_id = global_table_id
        self.global_table_name = global_table_name
        self.phy_table = phy_table

class UpdateGlobalTableResponse(DefaultJsonObject):
    def __init__(self, request_id=None):
        self.request_id = request_id
