# -*- coding: utf8 -*-# 

import six
from builtins import int

from tablestore.error import *
from tablestore.metadata import *
from tablestore.plainbuffer.plain_buffer_builder import *
import tablestore.protobuf.table_store_pb2 as pb2
import tablestore.protobuf.table_store_filter_pb2 as filter_pb2

INT32_MAX = 2147483647
INT32_MIN = -2147483648

PRIMARY_KEY_TYPE_MAP = {
    'INTEGER'   : pb2.INTEGER,
    'STRING'    : pb2.STRING,
    'BINARY'    : pb2.BINARY,
}

PRIMARY_KEY_OPTION_MAP = {
    PK_AUTO_INCR : pb2.AUTO_INCREMENT,
}

LOGICAL_OPERATOR_MAP = {
    LogicalOperator.NOT     : filter_pb2.LO_NOT,
    LogicalOperator.AND     : filter_pb2.LO_AND,
    LogicalOperator.OR      : filter_pb2.LO_OR,
}

COMPARATOR_TYPE_MAP = {
    ComparatorType.EQUAL          : filter_pb2.CT_EQUAL,
    ComparatorType.NOT_EQUAL      : filter_pb2.CT_NOT_EQUAL,
    ComparatorType.GREATER_THAN   : filter_pb2.CT_GREATER_THAN,
    ComparatorType.GREATER_EQUAL  : filter_pb2.CT_GREATER_EQUAL,
    ComparatorType.LESS_THAN      : filter_pb2.CT_LESS_THAN,
    ComparatorType.LESS_EQUAL     : filter_pb2.CT_LESS_EQUAL,
}

COLUMN_CONDITION_TYPE_MAP = {
    ColumnConditionType.COMPOSITE_COLUMN_CONDITION : filter_pb2.FT_COMPOSITE_COLUMN_VALUE,
    ColumnConditionType.SINGLE_COLUMN_CONDITION  : filter_pb2.FT_SINGLE_COLUMN_VALUE,
}

DIRECTION_MAP = {
    Direction.FORWARD           : pb2.FORWARD,
    Direction.BACKWARD          : pb2.BACKWARD,
}

ROW_EXISTENCE_EXPECTATION_MAP = {
    RowExistenceExpectation.IGNORE           : pb2.IGNORE,
    RowExistenceExpectation.EXPECT_EXIST     : pb2.EXPECT_EXIST ,
    RowExistenceExpectation.EXPECT_NOT_EXIST : pb2.EXPECT_NOT_EXIST ,
}

class OTSProtoBufferEncoder(object):

    def __init__(self, encoding):
        self.encoding = encoding

        self.api_encode_map = {
            'CreateTable'       : self._encode_create_table, 
            'DeleteTable'       : self._encode_delete_table, 
            'ListTable'         : self._encode_list_table,
            'UpdateTable'       : self._encode_update_table,
            'DescribeTable'     : self._encode_describe_table,
            'GetRow'            : self._encode_get_row,
            'PutRow'            : self._encode_put_row,
            'UpdateRow'         : self._encode_update_row,
            'DeleteRow'         : self._encode_delete_row,
            'BatchGetRow'       : self._encode_batch_get_row,
            'BatchWriteRow'     : self._encode_batch_write_row,
            'GetRange'          : self._encode_get_range
        }

    def _get_unicode(self, value):
        if isinstance(value, six.binary_type):
            return value.decode(self.encoding)
        elif isinstance(value, six.text_type):
            return value
        else:
            raise OTSClientError(
                "expect str or unicode type for string, not %s: %s" % (
                value.__class__.__name__, str(value))
            )

    def _get_int32(self, int32):
        if isinstance(int32, int):
            if int32 < INT32_MIN or int32 > INT32_MAX:
                raise OTSClientError("%s exceeds the range of int32" % int32)
            return int32
        else:
            raise OTSClientError(
                "expect int or long for the value, not %s"
                % int32.__class__.__name__
            )

    def _make_repeated_column_names(self, proto, columns_to_get):
        if columns_to_get is None:
            # if no column name is given, get all primary_key_columns and attribute_columns.
            return

        if not isinstance(columns_to_get, list) and not isinstance(columns_to_get, tuple):
            raise OTSClientError(
                "expect list or tuple for columns_to_get, not %s"
                % columns_to_get.__class__.__name__
            )

        for column_name in columns_to_get:
            proto.append(self._get_unicode(column_name))

    def _make_column_value(self, proto, value):
        # you have to put 'int' under 'bool' in the switch case
        # because a bool is also a int !!!

        if isinstance(value, six.text_type) or isinstance(value, six.text_type):
            string = self._get_unicode(value)
            proto.type = pb2.STRING
            proto.v_string = string
        elif isinstance(value, bool):
            proto.type = pb2.BOOLEAN
            proto.v_bool = value
        elif isinstance(value, int):
            proto.type = pb2.INTEGER
            proto.v_int = value
        elif isinstance(value, float):
            proto.type = pb2.DOUBLE
            proto.v_double = value
        elif isinstance(value, bytearray):
            proto.type = pb2.BINARY
            proto.v_binary = bytes(value)
        elif value is INF_MIN:
            proto.type = pb2.INF_MIN
        elif value is INF_MAX:
            proto.type = pb2.INF_MAX
        else:
            raise OTSClientError(
                "expect str, unicode, int, long, bool or float for colum value, not %s"
                % value.__class__.__name__
            )

    def _get_column_option(self, option):
        global PRIMARY_KEY_OPTION_MAP
        enum_map = PRIMARY_KEY_OPTION_MAP

        proto_option = enum_map.get(option)

        if proto_option != None:
            return proto_option
        else:
            raise OTSClientError(
                "primary_key_option should be one of [%s], not %s" % (
                    ", ".join(list(enum_map.keys())), str(option)
                )
            )

    def _get_column_type(self, type_str):
        global PRIMARY_KEY_TYPE_MAP
        enum_map = PRIMARY_KEY_TYPE_MAP

        proto_type = enum_map.get(type_str)

        if proto_type != None:
            return proto_type
        else:
            raise OTSClientError(
                "primary_key_type should be one of [%s], not %s" % (
                    ", ".join(sorted(list(enum_map.keys()))), str(type_str)
                )
            )

    def _make_composite_condition(self, condition):
        proto = filter_pb2.CompositeColumnValueFilter()

        # combinator
        global LOGICAL_OPERATOR_MAP
        enum_map = LOGICAL_OPERATOR_MAP

        proto.combinator = enum_map.get(condition.combinator)
        if proto.combinator is None:
            raise OTSClientError(
                "LogicalOperator should be one of [%s], not %s" % (
                    ", ".join(list(enum_map.keys())), str(condition.combinator)
                )
            )

        for sub in condition.sub_conditions:
            self._make_column_condition(proto.sub_filters.add(), sub)

        return proto.SerializeToString()

    def _make_relation_condition(self, condition):
        proto = filter_pb2.SingleColumnValueFilter()

        # comparator
        global COMPARATOR_TYPE_MAP
        enum_map = COMPARATOR_TYPE_MAP

        proto.comparator = enum_map.get(condition.comparator)
        if proto.comparator is None: 
            raise OTSClientError(
                "ComparatorType should be one of [%s], not %s" % (
                    ", ".join(list(enum_map.keys())), str(condition.comparator)
                )
            )

        proto.column_name = self._get_unicode(condition.column_name)
        proto.column_value = bytes(PlainBufferBuilder.serialize_column_value(condition.column_value))
        proto.filter_if_missing = not condition.pass_if_missing 
        proto.latest_version_only = condition.latest_version_only

        return proto.SerializeToString()

    def _make_column_condition(self, proto, column_condition):
        if column_condition == None:
            return

        if not isinstance(column_condition, ColumnCondition):
            raise OTSClientError(
                "column condition should be an instance of ColumnCondition, not %s" %
                column_condition.__class__.__name__
            )

        # type
        global COLUMN_CONDITION_TYPE_MAP
        enum_map = COLUMN_CONDITION_TYPE_MAP

        proto.type = enum_map.get(column_condition.get_type())
        if proto.type is None:
            raise OTSClientError(
                "column_condition_type should be one of [%s], not %s" % (
                    ", ".join(list(enum_map.keys())), str(column_condition.type)
                )
            )

        # condition
        if isinstance(column_condition, CompositeColumnCondition):
            proto.filter = self._make_composite_condition(column_condition)
        elif isinstance(column_condition, SingleColumnCondition):
            proto.filter = self._make_relation_condition(column_condition)
        else:
            raise OTSClientError(
                "expect CompositeColumnCondition, SingleColumnCondition but not %s"
                % column_condition.__class__.__name__
            )

    def _make_condition(self, proto, condition):

        if not isinstance(condition, Condition):
            raise OTSClientError(
                "condition should be an instance of Condition, not %s" %
                condition.__class__.__name__
            )
 
        global ROW_EXISTENCE_EXPECTATION_MAP
        enum_map = ROW_EXISTENCE_EXPECTATION_MAP

        expectation_str = self._get_unicode(condition.row_existence_expectation) 
        
        proto.row_existence = enum_map.get(expectation_str)
        if proto.row_existence is None:
            raise OTSClientError(
                "row_existence_expectation should be one of [%s], not %s" % (
                    ", ".join(list(enum_map.keys())), str(expectation_str)
                )
            )

        if condition.column_condition is not None:
            pb_filter = filter_pb2.Filter()
            self._make_column_condition(pb_filter, condition.column_condition)
            proto.column_condition = pb_filter.SerializeToString()

    def _get_direction(self, direction_str):
        global DIRECTION_MAP
        enum_map = DIRECTION_MAP

        proto_direction = enum_map.get(direction_str)
        if proto_direction != None:
            return proto_direction
        else:
            raise OTSClientError(
                "direction should be one of [%s], not %s" % (
                    ", ".join(list(enum_map.keys())), str(direction_str)
                )
            )

    def _make_column_schema(self, proto, schema_tuple):
        proto.name = self._get_unicode(schema_tuple[0])
        proto.type = self._get_column_type(schema_tuple[1])
        if len(schema_tuple) == 3:
            proto.option = self._get_column_option(schema_tuple[2])

    def _make_schemas_with_list(self, proto, schema_list):
        for schema_tuple in schema_list:
            if not isinstance(schema_tuple, tuple):
                raise OTSClientError(
                    "all schemas of primary keys should be tuple, not %s" % (
                        schema_tuple.__class__.__name__
                    )
                )
            schema_proto = proto.add()
            self._make_column_schema(schema_proto, schema_tuple)

    def _make_columns_with_dict(self, proto, column_dict):
        for name, value in column_dict.items():
            item = proto.add()
            item.name = self._get_unicode(name)
            self._make_column_value(item.value, value)

    def _make_update_of_attribute_columns_with_dict(self, proto, column_dict):
    
        if not isinstance(column_dict, dict):
            raise OTSClientError(
                "expect dict for 'update_of_attribute_columns', not %s" % (
                    column_dict.__class__.__name__
                )
            )

        for key, value in column_dict.items():
            if key == 'put':
                if not isinstance(column_dict[key], dict):
                    raise OTSClientError(
                        "expect dict for put operation in 'update_of_attribute_columns', not %s" % (
                            column_dict[key].__class__.__name__
                        )
                    )
                for name, value in column_dict[key].items():
                    item = proto.add()
                    item.type = pb2.PUT
                    item.name = self._get_unicode(name)
                    self._make_column_value(item.value, value)
            elif key == 'delete':
                if not isinstance(column_dict[key], list):
                    raise OTSClientError(
                        "expect list for delete operation in 'update_of_attribute_columns', not %s" % (
                            column_dict[key].__class__.__name__
                        )
                    )
                for name in column_dict[key]:
                    item = proto.add()
                    item.type = pb2.DELETE
                    item.name = self._get_unicode(name)
            else:
                raise OTSClientError(
                    "operation type in 'update_of_attribute_columns' should be 'put' or 'delete', not %s" % (
                        key
                    )
                )

    def _make_table_meta(self, proto, table_meta):
        if not isinstance(table_meta, TableMeta):
            raise OTSClientError(
                "table_meta should be an instance of TableMeta, not %s" 
                % table_meta.__class__.__name__
            )

        proto.table_name = self._get_unicode(table_meta.table_name)
        
        self._make_schemas_with_list(
            proto.primary_key,
            table_meta.schema_of_primary_key,
        )

    def _make_table_options(self, proto, table_options):
        if not isinstance(table_options, TableOptions):
            raise OTSClientError(
                "table_option should be an instance of TableOptions, not %s" 
                % table_options.__class__.__name__
            )
        if table_options.time_to_live is not None:
            if not isinstance(table_options.time_to_live, int):
                raise OTSClientError(
                    "time_to_live should be an instance of int, not %s" 
                    % table_options.time_to_live.__class__.__name__
                    )   
            proto.time_to_live = table_options.time_to_live

        if table_options.max_version is not None:
            if not isinstance(table_options.max_version, int):
                raise OTSClientError(
                    "max_version should be an instance of int, not %s" 
                    % table_options.max_version.__class__.__name__
                    )   
            proto.max_versions = table_options.max_version

        if table_options.max_time_deviation is not None:
            if not isinstance(table_options.max_time_deviation, int):
                raise OTSClientError(
                    "max_time_deviation should be an instance of TableOptions, not %s" 
                    % table_options.max_time_deviation.__class__.__name__
                    )   
            proto.deviation_cell_version_in_sec = table_options.max_time_deviation

    def _make_capacity_unit(self, proto, capacity_unit):

        if not isinstance(capacity_unit, CapacityUnit):
            raise OTSClientError(
                "capacity_unit should be an instance of CapacityUnit, not %s" 
                % capacity_unit.__class__.__name__
            )
        
        if capacity_unit.read is None or capacity_unit.write is None:
            raise OTSClientError("both of read and write of CapacityUnit are required")
        proto.read = self._get_int32(capacity_unit.read)
        proto.write = self._get_int32(capacity_unit.write)

    def _make_reserved_throughput(self, proto, reserved_throughput):

        if not isinstance(reserved_throughput, ReservedThroughput):
            raise OTSClientError(
                "reserved_throughput should be an instance of ReservedThroughput, not %s" 
                % reserved_throughput.__class__.__name__
            )
        
        self._make_capacity_unit(proto.capacity_unit, reserved_throughput.capacity_unit)

    def _make_update_capacity_unit(self, proto, capacity_unit):
        if not isinstance(capacity_unit, CapacityUnit):
            raise OTSClientError(
                "capacity_unit should be an instance of CapacityUnit, not %s" 
                % capacity_unit.__class__.__name__
            )
        
        if capacity_unit.read is None and capacity_unit.write is None:
            raise OTSClientError("at least one of read or write of CapacityUnit is required")
        if capacity_unit.read is not None:
            proto.read = self._get_int32(capacity_unit.read)
        if capacity_unit.write is not None:
            proto.write = self._get_int32(capacity_unit.write)

    def _make_update_reserved_throughput(self, proto, reserved_throughput):

        if not isinstance(reserved_throughput, ReservedThroughput):
            raise OTSClientError(
                "reserved_throughput should be an instance of ReservedThroughput, not %s" 
                % reserved_throughput.__class__.__name__
            )
        
        self._make_update_capacity_unit(proto.capacity_unit, reserved_throughput.capacity_unit)

    def _make_batch_get_row_internal(self, proto, request):
        for table_name, item in list(request.items.items()):
            table_item = proto.tables.add()
            table_item.table_name = self._get_unicode(item.table_name)
            self._make_repeated_column_names(table_item.columns_to_get, item.columns_to_get)

            if item.column_filter is not None:
                pb_filter = filter_pb2.Filter()
                self._make_column_condition(pb_filter, item.column_filter)
                table_item.filter = pb_filter.SerializeToString()

            for pk in item.primary_keys:
                table_item.primary_key.append(bytes(PlainBufferBuilder.serialize_primary_key(pk)))
            if item.token is not None:
                for token in item.token:
                    table_item.token.append(token)

            if item.max_version is not None:
                table_item.max_versions = item.max_version
            if item.time_range is not None:
                if isinstance(item.time_range, tuple):
                    table_item.time_range.start_time = item.time_range[0]
                    table_item.time_range.end_time = item.time_range[1]
                elif isinstance(item.time_range, int) or isinstance(item.time_range, int):
                    table_item.time_range.specific_time = item.time_range

            if item.start_column is not None:
                table_item.start_column = item.start_column
            if item.end_column is not None:
                table_item.end_column = item.end_column

    def _make_batch_get_row(self, proto, request):
        if isinstance(request, BatchGetRowRequest):
            self._make_batch_get_row_internal(proto, request) 
        else:
            raise OTSClientError("The request should be a instance of BatchGetRowRequest, not %d"%(len(request.__class__.__name__)))

    def _make_put_row_item(self, proto, put_row_item):
        condition = put_row_item.condition
        if condition is None:
            condition = Condition(RowExistenceExpectation.IGNORE, None)
        self._make_condition(proto.condition, condition)
        if put_row_item.return_type == ReturnType.RT_PK:
            proto.return_content.return_type = pb2.RT_PK

        proto.row_change = bytes(PlainBufferBuilder.serialize_for_put_row(
                put_row_item.row.primary_key, put_row_item.row.attribute_columns))
        proto.type = pb2.PUT
        return proto

    def _make_update_row_item(self, proto, update_row_item):
        condition = update_row_item.condition
        if condition is None:
            condition = Condition(RowExistenceExpectation.IGNORE, None)
        self._make_condition(proto.condition, condition)

        if update_row_item.return_type == ReturnType.RT_PK:
            proto.return_content.return_type = pb2.RT_PK

        proto.row_change = bytes(PlainBufferBuilder.serialize_for_update_row(
                update_row_item.row.primary_key, update_row_item.row.attribute_columns))
        proto.type = pb2.UPDATE
        return proto

    def _make_delete_row_item(self, proto, delete_row_item):
        condition = delete_row_item.condition
        if condition is None:
            condition = Condition(RowExistenceExpectation.IGNORE, None)
        self._make_condition(proto.condition, condition)

        if delete_row_item.return_type == ReturnType.RT_PK:
            proto.return_content.return_type = pb2.RT_PK

        proto.row_change = bytes(PlainBufferBuilder.serialize_for_delete_row(delete_row_item.row.primary_key))
        proto.type = pb2.DELETE
        return proto

    def _make_batch_write_row_internal(self, proto, request):
        for table_name, item in list(request.items.items()):
            table_item = proto.tables.add()
            table_item.table_name = self._get_unicode(item.table_name)

            for row_item in item.row_items:
                if row_item.type == BatchWriteRowType.PUT:
                    row = table_item.rows.add()
                    self._make_put_row_item(row, row_item)

                if row_item.type == BatchWriteRowType.UPDATE:
                    row = table_item.rows.add()
                    self._make_update_row_item(row, row_item)

                if row_item.type == BatchWriteRowType.DELETE:
                    row = table_item.rows.add()
                    self._make_delete_row_item(row, row_item)


    def _make_batch_write_row(self, proto, request):
        if isinstance(request, BatchWriteRowRequest):
            self._make_batch_write_row_internal(proto, request) 
        else:
            raise OTSClientError("The request should be a instance of MultiTableInBatchWriteRowItem, not %d"%(len(request.__class__.__name__)))
    
             
    def _encode_create_table(self, table_meta, table_options, reserved_throughput):
        proto = pb2.CreateTableRequest()
        self._make_table_meta(proto.table_meta, table_meta)
        self._make_reserved_throughput(proto.reserved_throughput, reserved_throughput)
        self._make_table_options(proto.table_options, table_options)
        return proto

    def _encode_delete_table(self, table_name):
        proto = pb2.DeleteTableRequest()
        proto.table_name = self._get_unicode(table_name)
        return proto

    def _encode_list_table(self):
        proto = pb2.ListTableRequest()
        return proto

    def _encode_update_table(self, table_name, table_options, reserved_throughput):
        proto = pb2.UpdateTableRequest()
        proto.table_name = self._get_unicode(table_name)
        if reserved_throughput is not None:
            self._make_update_reserved_throughput(proto.reserved_throughput, reserved_throughput)
        if table_options is not None:
            self._make_table_options(proto.table_options, table_options)
        return proto

    def _encode_describe_table(self, table_name):
        proto = pb2.DescribeTableRequest()
        proto.table_name = self._get_unicode(table_name)
        return proto

    def _encode_get_row(self, table_name, primary_key, columns_to_get, column_filter, 
                        max_version, time_range, start_column, end_column, token):
        proto = pb2.GetRowRequest()
        proto.table_name = self._get_unicode(table_name)
        self._make_repeated_column_names(proto.columns_to_get, columns_to_get)

        if column_filter is not None:
            pb_filter = filter_pb2.Filter()
            self._make_column_condition(pb_filter, column_filter)
            proto.filter = pb_filter.SerializeToString()

        proto.primary_key = bytes(PlainBufferBuilder.serialize_primary_key(primary_key))
        if max_version is not None:
            proto.max_versions = max_version
        if time_range is not None:
            if isinstance(time_range, tuple):
                proto.time_range.start_time = time_range[0]
                proto.time_range.end_time = time_range[1]
            elif isinstance(time_range, int) or isinstance(time_range, int):
                proto.time_range.specific_time = time_range

        if start_column is not None:
            proto.start_column = start_column
        if end_column is not None:
            proto.end_column = end_column
        if token is not None:
            proto.token = token

        return proto

    def _encode_put_row(self, table_name, row, condition, return_type):
        proto = pb2.PutRowRequest()
        proto.table_name = self._get_unicode(table_name)
        if condition is None:
            condition = Condition(RowExistenceExpectation.IGNORE, None)
        self._make_condition(proto.condition, condition)
        if return_type == ReturnType.RT_PK:
            proto.return_content.return_type = pb2.RT_PK

        proto.row = bytes(PlainBufferBuilder.serialize_for_put_row(row.primary_key, row.attribute_columns))
        return proto

    def _encode_update_row(self, table_name, row, condition, return_type):
        proto = pb2.UpdateRowRequest()
        proto.table_name = self._get_unicode(table_name)
        if condition is None:
            condition = Condition(RowExistenceExpectation.IGNORE, None)
        self._make_condition(proto.condition, condition)

        if return_type == ReturnType.RT_PK:
            proto.return_content.return_type = pb2.RT_PK

        proto.row_change = bytes(PlainBufferBuilder.serialize_for_update_row(row.primary_key, row.attribute_columns))
        return proto

    def _encode_delete_row(self, table_name, row, condition, return_type):
        proto = pb2.DeleteRowRequest()
        proto.table_name = self._get_unicode(table_name)
        if condition is None:
            condition = Condition(RowExistenceExpectation.IGNORE, None)
        self._make_condition(proto.condition, condition)

        if return_type == ReturnType.RT_PK:
            proto.return_content.return_type = pb2.RT_PK

        proto.primary_key = bytes(PlainBufferBuilder.serialize_for_delete_row(row.primary_key))
        return proto

    def _encode_batch_get_row(self, request):
        proto = pb2.BatchGetRowRequest()
        self._make_batch_get_row(proto, request)
        return proto

    def _encode_batch_write_row(self, request):
        proto = pb2.BatchWriteRowRequest()
        self._make_batch_write_row(proto, request)
        return proto

    def _encode_get_range(self, table_name, direction, 
                inclusive_start_primary_key, exclusive_end_primary_key, 
                columns_to_get, limit, column_filter,
                max_version, time_range, start_column,
                end_column, token):
        proto = pb2.GetRangeRequest()
        proto.table_name = self._get_unicode(table_name)
        proto.direction = self._get_direction(direction)
        self._make_repeated_column_names(proto.columns_to_get, columns_to_get)

        proto.inclusive_start_primary_key = bytes(PlainBufferBuilder.serialize_primary_key(inclusive_start_primary_key))
        proto.exclusive_end_primary_key = bytes(PlainBufferBuilder.serialize_primary_key(exclusive_end_primary_key))

        if column_filter is not None:
            pb_filter = filter_pb2.Filter()
            self._make_column_condition(pb_filter, column_filter)
            proto.filter = pb_filter.SerializeToString()

        if limit is not None:
            proto.limit = self._get_int32(limit)
        if max_version is not None:
            proto.max_versions = max_version
        if time_range is not None:
            if isinstance(time_range, tuple):
                proto.time_range.start_time = time_range[0]
                proto.time_range.end_time = time_range[1]
            elif isinstance(time_range, int):
                proto.time_range.specific_time = time_range 
        if start_column is not None:
            proto.start_column = start_column
        if end_column is not None:
            proto.end_colun = end_column
        if token is not None:
            proto.token = token
        return proto

    def encode_request(self, api_name, *args, **kwargs):
        if api_name not in self.api_encode_map:
            raise OTSClientError("No PB encode method for API %s" % api_name)

        handler = self.api_encode_map[api_name]
        return handler(*args, **kwargs)
