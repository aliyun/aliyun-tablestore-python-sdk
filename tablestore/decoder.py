# -*- coding: utf8 -*-

import google.protobuf.text_format as text_format

from tablestore.metadata import *
from tablestore.plainbuffer.plain_buffer_builder import *

import tablestore.protobuf.table_store_pb2 as pb
import tablestore.protobuf.table_store_filter_pb2 as filter_pb

class OTSProtoBufferDecoder(object):

    def __init__(self, encoding):
        self.encoding = encoding

        self.api_decode_map = {
            'CreateTable'       : self._decode_create_table,
            'ListTable'         : self._decode_list_table,
            'DeleteTable'       : self._decode_delete_table,
            'DescribeTable'     : self._decode_describe_table,
            'UpdateTable'       : self._decode_update_table,
            'GetRow'            : self._decode_get_row,
            'PutRow'            : self._decode_put_row,
            'UpdateRow'         : self._decode_update_row,
            'DeleteRow'         : self._decode_delete_row,
            'BatchGetRow'       : self._decode_batch_get_row,
            'BatchWriteRow'     : self._decode_batch_write_row,
            'GetRange'          : self._decode_get_range,
        }

    def _parse_string(self, string):
        if string == '':
            return None
        else:
            return string

    def _parse_column_type(self, column_type_enum):
        reverse_enum_map = {
            pb.INTEGER : 'INTEGER',
            pb.STRING  : 'STRING',
            pb.BINARY  : 'BINARY'
        }
        if column_type_enum in reverse_enum_map:
            return reverse_enum_map[column_type_enum]
        else:
            raise OTSClientError("invalid value for column type: %s" % str(column_type_enum))

    def _parse_column_option(self, column_option_enum):
        reverse_enum_map = {
            pb.AUTO_INCREMENT : PK_AUTO_INCR,
        }
        if column_option_enum in reverse_enum_map:
            return reverse_enum_map[column_option_enum]
        else:
            raise OTSClientError("invalid value for column option: %s" % str(column_option_enum))        

    def _parse_value(self, proto):
        if proto.type == pb.INTEGER:
            return proto.v_int
        elif proto.type == pb.STRING:
            return proto.v_string
        elif proto.type == pb.BOOLEAN:
            return proto.v_bool
        elif proto.type == pb.DOUBLE:
            return proto.v_double
        elif proto.type == pb.BINARY:
            return bytearray(proto.v_binary)
        else:
            raise OTSClientError("invalid column value type: %s" % str(proto.type))

    def _parse_schema_list(self, proto):
        ret = []
        for item in proto:
            if item.HasField('option'):
                ret.append((item.name, self._parse_column_type(item.type), self._parse_column_option(item.option)))
            else:
                ret.append((item.name, self._parse_column_type(item.type)))

        return ret

    def _parse_column_dict(self, proto):
        ret = {}
        for item in proto:
            ret[item.name] = self._parse_value(item.value)
        return ret

    def _parse_row(self, proto):
        return (
            self._parse_column_dict(proto.primary_key_columns),
            self._parse_column_dict(proto.attribute_columns)
        )

    def _parse_row_list(self, proto):
        row_list = [] 
        for row_item in proto:
            row_list.append(self._parse_row(row_item))
        return row_list

    def _parse_capacity_unit(self, proto):
        if proto is None:
            capacity_unit = None
        else:
            cu_read = proto.read if proto.HasField('read') else 0
            cu_write = proto.write if proto.HasField('write') else 0
            capacity_unit = CapacityUnit(cu_read, cu_write)
        return capacity_unit

    def _parse_reserved_throughput_details(self, proto):
        last_decrease_time = proto.last_decrease_time if proto.HasField('last_decrease_time') else None
        capacity_unit = self._parse_capacity_unit(proto.capacity_unit)

        reserved_throughput_details = ReservedThroughputDetails(
            capacity_unit,
            proto.last_increase_time, 
            last_decrease_time
        )
        return reserved_throughput_details

    def _parse_table_options(self, proto):
        time_to_live = proto.time_to_live 
        max_versions = proto.max_versions
        max_deviation_time  = proto.deviation_cell_version_in_sec
        return TableOptions(time_to_live, max_versions, max_deviation_time)


    def _parse_get_row_item(self, proto, table_name):
        row_list = []
        for row_item in proto:
            primary_key_columns = None 
            attribute_columns = None

            if row_item.is_ok:
                error_code = None
                error_message = None
                capacity_unit = self._parse_capacity_unit(row_item.consumed.capacity_unit)

                if len(row_item.row) != 0:
                    inputStream = PlainBufferInputStream(row_item.row)
                    codedInputStream = PlainBufferCodedInputStream(inputStream)
                    primary_key_columns, attribute_columns = codedInputStream.read_row()
            else:
                error_code = row_item.error.code
                error_message = row_item.error.message if row_item.error.HasField('message') else ''
                if row_item.HasField('consumed'):
                    capacity_unit = self._parse_capacity_unit(row_item.consumed.capacity_unit)
                else:
                    capacity_unit = None

            row_data_item = RowDataItem(
                row_item.is_ok, error_code, error_message,
                table_name,
                capacity_unit, primary_key_columns, attribute_columns
            )
            row_list.append(row_data_item)
        
        return row_list

    def _parse_batch_get_row(self, proto):
        rows = []
        for table_item in proto:
            rows.append(self._parse_get_row_item(table_item.rows, table_item.table_name)) 
        return rows

    def _parse_write_row_item(self, row_item):
        primary_key_columns = None 
        attribute_columns = None

        if row_item.is_ok:
            error_code = None
            error_message = None
            consumed = self._parse_capacity_unit(row_item.consumed.capacity_unit)

            if len(row_item.row) != 0:
                inputStream = PlainBufferInputStream(row_item.row)
                codedInputStream = PlainBufferCodedInputStream(inputStream)
                primary_key_columns, attribute_columns = codedInputStream.read_row()
        else:
            error_code = row_item.error.code
            error_message = row_item.error.message if row_item.error.HasField('message') else ''
            if row_item.HasField('consumed'):
                consumed = self._parse_capacity_unit(row_item.consumed.capacity_unit)
            else:
                consumed = None

        write_row_item = BatchWriteRowResponseItem(
            row_item.is_ok, error_code, error_message, consumed, primary_key_columns
            )
        return write_row_item

    def _parse_batch_write_row(self, proto):
        result_list = {}
        for table_item in proto:
            table_name = table_item.table_name
            result_list[table_name] = []

            for row_item in table_item.rows:
                row = self._parse_write_row_item(row_item)
                result_list[table_name].append(row)
        return result_list

    def _decode_create_table(self, body):
        proto = pb.CreateTableResponse()
        proto.ParseFromString(body)
        return None, proto

    def _decode_list_table(self, body):
        proto = pb.ListTableResponse()
        proto.ParseFromString(body)
        names = tuple(proto.table_names)
        return names, proto

    def _decode_delete_table(self, body):
        proto = pb.DeleteTableResponse()
        proto.ParseFromString(body)
        return None, proto
        
    def _decode_describe_table(self, body):
        proto = pb.DescribeTableResponse()
        proto.ParseFromString(body)

        table_meta = TableMeta(
            proto.table_meta.table_name,
            self._parse_schema_list(
                proto.table_meta.primary_key
            )
        )
        
        reserved_throughput_details = self._parse_reserved_throughput_details(proto.reserved_throughput_details)
        table_options = self._parse_table_options(proto.table_options)
        describe_table_response = DescribeTableResponse(table_meta, table_options, reserved_throughput_details)
        return describe_table_response, proto

    def _decode_update_table(self, body):
        proto = pb.UpdateTableResponse()
        proto.ParseFromString(body)

        reserved_throughput_details = self._parse_reserved_throughput_details(proto.reserved_throughput_details)
        table_options = self._parse_table_options(proto.table_options)
        update_table_response = UpdateTableResponse(reserved_throughput_details, table_options)

        return update_table_response, proto

    def _decode_get_row(self, body):
        proto = pb.GetRowResponse()
        proto.ParseFromString(body)

        consumed = self._parse_capacity_unit(proto.consumed.capacity_unit)
        next_token = proto.next_token

        return_row = None

        if len(proto.row) != 0:
            inputStream = PlainBufferInputStream(proto.row)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            primary_key, attributes = codedInputStream.read_row()
            return_row = Row(primary_key, attributes)

        return (consumed, return_row, next_token), proto

    def _decode_put_row(self, body):
        proto = pb.PutRowResponse()
        proto.ParseFromString(body)

        consumed = self._parse_capacity_unit(proto.consumed.capacity_unit)

        return_row = None

        if len(proto.row) != 0:
            inputStream = PlainBufferInputStream(proto.row)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            primary_key, attribute_columns = codedInputStream.read_row()
            return_row = Row(primary_key, attribute_columns)

        return (consumed, return_row), proto

    def _decode_update_row(self, body):
        proto = pb.UpdateRowResponse()
        proto.ParseFromString(body)

        consumed = self._parse_capacity_unit(proto.consumed.capacity_unit)
        
        return_row = None

        if len(proto.row) != 0:
            inputStream = PlainBufferInputStream(proto.row)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            primary_key, attribute_columns = codedInputStream.read_row()
            return_row = Row(primary_key, attribute_columns)

        return (consumed, return_row), proto

    def _decode_delete_row(self, body):
        proto = pb.DeleteRowResponse()
        proto.ParseFromString(body)

        consumed = self._parse_capacity_unit(proto.consumed.capacity_unit)

        return_row = None

        if len(proto.row) != 0:
            inputStream = PlainBufferInputStream(proto.row)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            primary_key, attribute_columns = codedInputStream.read_row()
            return_row = Row(primary_key, attribute_columns)

        return (consumed, return_row), proto


    def _decode_batch_get_row(self, body):
        proto = pb.BatchGetRowResponse()
        proto.ParseFromString(body)

        rows = self._parse_batch_get_row(proto.tables)
        return rows, proto

    def _decode_batch_write_row(self, body):
        proto = pb.BatchWriteRowResponse()
        proto.ParseFromString(body)

        rows = self._parse_batch_write_row(proto.tables)
        return rows, proto

    def _decode_get_range(self, body):
        proto = pb.GetRangeResponse()
        proto.ParseFromString(body)
        
        capacity_unit = self._parse_capacity_unit(proto.consumed.capacity_unit)

        next_start_pk = None
        row_list = []
        if len(proto.next_start_primary_key) != 0:
            inputStream = PlainBufferInputStream(proto.next_start_primary_key)
            codedInputStream = PlainBufferCodedInputStream(inputStream)            
            next_start_pk,att = codedInputStream.read_row()

        if len(proto.rows) != 0:
            inputStream = PlainBufferInputStream(proto.rows)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            row_list = codedInputStream.read_rows()
        
        next_token = proto.next_token
            
        return (capacity_unit, next_start_pk, row_list, next_token), proto

    def decode_response(self, api_name, response_body):
        if api_name not in self.api_decode_map:
            raise OTSClientError("No PB decode method for API %s" % api_name)

        handler = self.api_decode_map[api_name]
        return handler(response_body)

