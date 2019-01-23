# -*- coding: utf8 -*-

import sys
import six
import struct
from builtins import int
from tablestore.const import *
from tablestore.metadata import *
from tablestore.error import *
from .plain_buffer_consts import *
from .plain_buffer_crc8 import *
from .plain_buffer_consts import *

class PlainBufferCodedInputStream(object):
    def __init__(self, input_stream):
        self.input_stream = input_stream

    def read_tag(self):
        return self.input_stream.read_tag()

    def check_last_tag_was(self, tag):
        return self.input_stream.check_last_tag_was(tag)

    def get_last_tag(self):
        return self.input_stream.get_last_tag()

    def read_header(self):
        return self.input_stream.read_int32()

    def read_primary_key_value(self, cell_check_sum):
        if not self.check_last_tag_was(TAG_CELL_VALUE):
            raise OTSClientError("Expect TAG_CELL_VALUE but it was " + str(self.get_last_tag()))

        self.input_stream.read_raw_little_endian32()        
        column_type = ord(self.input_stream.read_raw_byte())
        if column_type == VT_INTEGER:
            int64_value = self.input_stream.read_int64()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, int64_value)
            self.read_tag()
            return (int64_value, cell_check_sum)
        elif column_type == VT_STRING:
            value_size = self.input_stream.read_int32()
            string_value = self.input_stream.read_utf_string(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
            self.read_tag()
            return (string_value, cell_check_sum)
        elif column_type == VT_BLOB:
            value_size = self.input_stream.read_int32() 
            binary_value = self.input_stream.read_bytes(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value)            
            self.read_tag()
            return (bytearray(binary_value), cell_check_sum)
        else:
            raise OTSClientError("Unsupported primary key type:" + str(column_type))

    def read_column_value(self, cell_check_sum):
        if not self.check_last_tag_was(TAG_CELL_VALUE):
            raise OTSClientError("Expect TAG_CELL_VALUE but it was " + str(self.get_last_tag()))
        self.input_stream.read_raw_little_endian32()
        column_type = ord(self.input_stream.read_raw_byte())
        if column_type == VT_INTEGER:
            int64_value = self.input_stream.read_int64()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, int64_value)
            self.read_tag()
            return (int64_value, cell_check_sum)
        elif column_type == VT_STRING:
            value_size = self.input_stream.read_int32()
            string_value = self.input_stream.read_utf_string(value_size)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
            self.read_tag()
            return (string_value, cell_check_sum)
        elif column_type == VT_BLOB:
            value_size = self.input_stream.read_int32() 
            binary_value = self.input_stream.read_bytes(value_size)            
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value)            
            self.read_tag()
            return (bytearray(binary_value), cell_check_sum)
        elif column_type == VT_BOOLEAN:
            bool_value = self.input_stream.read_boolean()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BOOLEAN)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, bool_value)
            self.read_tag()
            return (bool_value, cell_check_sum)
        elif column_type == VT_DOUBLE:
            double_int = self.input_stream.read_double()
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_DOUBLE)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, double_int)
            self.read_tag()

            if const.SYS_BITS == 64:
                double_value, = struct.unpack('d', struct.pack('q', double_int))
            elif const.SYS_BITS == 32:
                double_value, = struct.unpack('d', struct.pack('l', double_int))
            else:
                double_value, = struct.unpack('d', struct.pack('l', double_int))
            return (double_value, cell_check_sum)
        else:
            raise OTSClientError("Unsupported column type: " + str(column_type))

    def read_primary_key_column(self, row_check_sum):
        if not self.check_last_tag_was(TAG_CELL):
            raise OTSClientError("Expect TAG_CELL but it was " + str(self.get_last_tag()))
        self.read_tag()

        if not self.check_last_tag_was(TAG_CELL_NAME):
            raise OTSClientError("Expect TAG_CELL_NAME but it was " + str(self.get_last_tag()))
        
        cell_check_sum = 0
        name_size = self.input_stream.read_raw_little_endian32()
        column_name = self.input_stream.read_utf_string(name_size)
        cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, column_name)
        self.read_tag()
        
        if not self.check_last_tag_was(TAG_CELL_VALUE):
            raise OTSClientError("Expect TAG_CELL_VALUE but it was " + str(self.get_last_tag()))
        
        primary_key_value, cell_check_sum = self.read_primary_key_value(cell_check_sum)
        
        if self.get_last_tag() == TAG_CELL_CHECKSUM:
            check_sum = ord(self.input_stream.read_raw_byte())
            if check_sum != cell_check_sum:
                raise OTSClientError("Checksum mismatch. expected:" + str(check_sum) + ",actual:" + str(cell_check_sum))
            self.read_tag()
        else:
            raise OTSClientError("Expect TAG_CELL_CHECKSUM but it was " + str(self.get_last_tag()))
        
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return (column_name, primary_key_value, row_check_sum)

    def read_column(self, row_check_sum):
        if not self.check_last_tag_was(TAG_CELL):
            raise OTSClientError("Expect TAG_CELL but it was " + str(self.get_last_tag()))
        self.read_tag()

        if not self.check_last_tag_was(TAG_CELL_NAME):
            raise OTSClientError("Expect TAG_CELL_NAME but it was " + str(self.get_last_tag()))
                   
        cell_check_sum = 0
        column_name = None
        column_value = None
        timestamp = None
        name_size = self.input_stream.read_raw_little_endian32()
        column_name = self.input_stream.read_utf_string(name_size)
        cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, column_name)
        self.read_tag()
    
        if self.get_last_tag() == TAG_CELL_VALUE:
            column_value, cell_check_sum = self.read_column_value(cell_check_sum)
        # skip CELL_TYPE
        if self.get_last_tag() == TAG_CELL_TYPE:
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, cell_type)
            self.read_tag()
        
        if self.get_last_tag() == TAG_CELL_TIMESTAMP:
            timestamp = self.input_stream.read_int64()
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
            self.read_tag()

        if self.get_last_tag() == TAG_CELL_CHECKSUM:
            check_sum = ord(self.input_stream.read_raw_byte())
            if check_sum != cell_check_sum:                
                raise OTSClientError("Checksum mismatch. expected:" + str(check_sum) + ",actual:" + str(cell_check_sum))
            self.read_tag()
        else:
            raise OTSClientError("Expect TAG_CELL_CHECKSUM but it was " + str(self.get_last_tag()))
        
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return column_name, column_value, timestamp, row_check_sum

    def read_row_without_header(self):
        row_check_sum = 0
        primary_key = []
        attributes = []
        
        if not self.check_last_tag_was(TAG_ROW_PK):
            raise OTSClientError("Expect TAG_ROW_PK but it was " + str(self.get_last_tag()))

        self.read_tag()
        
        while self.check_last_tag_was(TAG_CELL):
            (name, value, row_check_sum) = self.read_primary_key_column(row_check_sum)
            primary_key.append((name, value))

        if self.check_last_tag_was(TAG_ROW_DATA):
            self.read_tag()
            while self.check_last_tag_was(TAG_CELL):
                column_name, column_value, timestamp, row_check_sum = self.read_column(row_check_sum)
                attributes.append((column_name, column_value, timestamp))

        if self.check_last_tag_was(TAG_DELETE_ROW_MARKER):
            self.read_tag()
            row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, 1)
        else:
            row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, 0)

        if self.check_last_tag_was(TAG_ROW_CHECKSUM):
            check_sum = ord(self.input_stream.read_raw_byte())
            if check_sum != row_check_sum:
                raise OTSClientError("Checksum is mismatch.")
            self.read_tag()
        else:
            raise OTSClientError("Expect TAG_ROW_CHECKSUM but it was " + str(self.get_last_tag()))
        
        return primary_key, attributes

    def read_row(self):
        if self.read_header() != HEADER:
            raise OTSClientError("Invalid header from plain buffer.")
        self.read_tag()
        return self.read_row_without_header()

    def read_rows(self):
        if self.read_header() != HEADER:
            raise OTSClientError("Invalid header from plain buffer.")
        self.read_tag()
        
        row_list = []
        while not self.input_stream.is_at_end():
            (pk, attr) = self.read_row_without_header()
            row_list.append(Row(pk, attr))
        return row_list

class PlainBufferCodedOutputStream(object):
    def __init__(self, output_stream):
        self.output_stream = output_stream

    def write_header(self):
        self.output_stream.write_raw_little_endian32(HEADER)

    def write_tag(self, tag):
        self.output_stream.write_raw_byte(tag)

    def write_cell_name(self, name, cell_check_sum):
        self.write_tag(TAG_CELL_NAME)
        self.output_stream.write_raw_little_endian32(len(name))
        self.output_stream.write_bytes(name)
        cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, name)
        return cell_check_sum

    def write_primary_key_value(self, value, cell_check_sum):
        self.write_tag(TAG_CELL_VALUE)
        if value is INF_MIN:
            self.output_stream.write_raw_little_endian32(1)
            self.output_stream.write_raw_byte(VT_INF_MIN)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INF_MIN)
        elif value is INF_MAX:
            self.output_stream.write_raw_little_endian32(1)
            self.output_stream.write_raw_byte(VT_INF_MAX)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INF_MAX)
        elif value is PK_AUTO_INCR:
            self.output_stream.write_raw_little_endian32(1)
            self.output_stream.write_raw_byte(VT_AUTO_INCREMENT)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_AUTO_INCREMENT)
        elif isinstance(value, int):
            self.output_stream.write_raw_little_endian32(1 + const.LITTLE_ENDIAN_64_SIZE)
            self.output_stream.write_raw_byte(VT_INTEGER)
            self.output_stream.write_raw_little_endian64(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, value)
        elif isinstance(value, six.text_type) or isinstance(value, six.binary_type):
            if isinstance(value, six.text_type):
                string_value = value.encode('utf-8')
            else:
                string_value = value
                
            prefix_length = const.LITTLE_ENDIAN_32_SIZE + 1
            self.output_stream.write_raw_little_endian32(prefix_length + len(string_value))
            self.output_stream.write_raw_byte(VT_STRING)
            self.output_stream.write_raw_little_endian32(len(string_value))
            self.output_stream.write_bytes(string_value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(string_value))
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
        elif isinstance(value, bytearray):
            binary_value = value
            prefix_length = const.LITTLE_ENDIAN_32_SIZE + 1

            self.output_stream.write_raw_little_endian32(prefix_length + len(binary_value))
            self.output_stream.write_raw_byte(VT_BLOB)
            self.output_stream.write_raw_little_endian32(len(binary_value))
            self.output_stream.write_bytes(binary_value)

            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(binary_value))
            cell_check_sum = PlainBufferCrc8.crc_bytes(cell_check_sum, binary_value)
        else:
            raise OTSClientError("Unsupported primary key type: " + type(value)) 
        return cell_check_sum

    def write_column_value_with_checksum(self, value, cell_check_sum):
        self.write_tag(TAG_CELL_VALUE)
        if isinstance(value, bool):
            self.output_stream.write_raw_little_endian32(2)
            self.output_stream.write_raw_byte(VT_BOOLEAN)
            self.output_stream.write_boolean(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BOOLEAN)
            if value:
                cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, 1)
            else:
                cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, 0)
        elif isinstance(value, int):
            self.output_stream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
            self.output_stream.write_raw_byte(VT_INTEGER)
            self.output_stream.write_raw_little_endian64(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, value)
        elif isinstance(value, six.text_type) or isinstance(value, six.binary_type):
            if isinstance(value, six.text_type):
                value = value.encode('utf-8')
            prefix_length = LITTLE_ENDIAN_32_SIZE + 1 
            self.output_stream.write_raw_little_endian32(prefix_length + len(value)) 
            self.output_stream.write_raw_byte(VT_STRING)
            self.output_stream.write_raw_little_endian32(len(value))
            self.output_stream.write_bytes(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(value))
            cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, value)
        elif isinstance(value, bytearray):
            prefix_length = LITTLE_ENDIAN_32_SIZE + 1
            self.output_stream.write_raw_little_endian32(prefix_length + len(value))
            self.output_stream.write_raw_byte(VT_BLOB)
            self.output_stream.write_raw_little_endian32(len(value))
            self.output_stream.write_bytes(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
            cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, len(value))
            cell_check_sum = PlainBufferCrc8.crc_bytes(cell_check_sum, value)
        elif isinstance(value, float):
            if const.SYS_BITS == 64:
                double_in_long, = struct.unpack("q", struct.pack("d", value))
            elif const.SYS_BITS == 32:
                double_in_long, = struct.unpack("l", struct.pack("d", value))
            else:
                double_in_long, = struct.unpack("l", struct.pack("d", value))
            self.output_stream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
            self.output_stream.write_raw_byte(VT_DOUBLE)
            self.output_stream.write_double(value)
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_DOUBLE)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, double_in_long)
        else:
            raise OTSClientError("Unsupported column type: " + str(type(value)))
        return cell_check_sum

    def write_column_value(self, value):
        if isinstance(value, bool):
            self.output_stream.write_raw_byte(VT_BOOLEAN)
            self.output_stream.write_boolean(value)
        elif isinstance(value, int):
            self.output_stream.write_raw_byte(VT_INTEGER)
            self.output_stream.write_raw_little_endian64(value)
        elif isinstance(value, six.text_type) or isinstance(value, six.binary_type):
            if isinstance(value, six.text_type):
                value = value.encode('utf-8')
            self.output_stream.write_raw_byte(VT_STRING)
            self.output_stream.write_raw_little_endian32(len(value))
            self.output_stream.write_bytes(value)
        elif isinstance(value, bytearray):
            self.output_stream.write_raw_byte(VT_BLOB)
            self.output_stream.write_raw_little_endian32(len(value))
            self.output_stream.write_bytes(value)
        elif isinstance(value, float):  
            self.output_stream.write_raw_byte(VT_DOUBLE)
            self.output_stream.write_double(value)
        else:
            raise OTSClientError("Unsupported column type: " + str(type(value)))

    def write_primary_key_column(self, pk_name, pk_value, row_check_sum):
        cell_check_sum = 0
        self.write_tag(TAG_CELL)
        cell_check_sum = self.write_cell_name(pk_name, cell_check_sum)
        cell_check_sum = self.write_primary_key_value(pk_value, cell_check_sum)
        self.write_tag(TAG_CELL_CHECKSUM)
        self.output_stream.write_raw_byte(cell_check_sum)
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return row_check_sum

    def write_column(self, column_name, column_value, timestamp, row_check_sum):
        cell_check_sum = 0
        self.write_tag(TAG_CELL)
        cell_check_sum = self.write_cell_name(column_name, cell_check_sum)
        cell_check_sum = self.write_column_value_with_checksum(column_value, cell_check_sum)

        if timestamp is not None:
            self.write_tag(TAG_CELL_TIMESTAMP)
            self.output_stream.write_raw_little_endian64(timestamp)
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
        self.write_tag(TAG_CELL_CHECKSUM)
        self.output_stream.write_raw_byte(cell_check_sum)
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return row_check_sum

    def write_update_column(self, update_type, column_name, column_value, row_check_sum):
        update_type = update_type.upper()
        cell_check_sum = 0
        self.write_tag(TAG_CELL)
        cell_check_sum = self.write_cell_name(column_name, cell_check_sum)
        timestamp = None
        if column_value is not None:
            if isinstance(column_value, tuple):
                if column_value[0] is not None:
                    cell_check_sum = self.write_column_value_with_checksum(column_value[0], cell_check_sum)
                if column_value[1] is not None:
                    timestamp = column_value[1]
            else:
                cell_check_sum = self.write_column_value_with_checksum(column_value, cell_check_sum)
        if update_type == UpdateType.DELETE:
            self.write_tag(TAG_CELL_TYPE)
            self.output_stream.write_raw_byte(const.DELETE_ONE_VERSION)
        elif update_type == UpdateType.DELETE_ALL:
            self.write_tag(TAG_CELL_TYPE)
            self.output_stream.write_raw_byte(const.DELETE_ALL_VERSION)
        elif update_type == UpdateType.INCREMENT:
            self.write_tag(TAG_CELL_TYPE)
            self.output_stream.write_raw_byte(const.INCREMENT)

        if timestamp is not None:
            self.write_tag(TAG_CELL_TIMESTAMP)
            self.output_stream.write_raw_little_endian64(timestamp)

        if timestamp is not None:
            cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
        if update_type == UpdateType.DELETE:
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, const.DELETE_ONE_VERSION)
        if update_type == UpdateType.DELETE_ALL:
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, const.DELETE_ALL_VERSION)
        if update_type == UpdateType.INCREMENT:
            cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, const.INCREMENT)

        self.write_tag(TAG_CELL_CHECKSUM)
        self.output_stream.write_raw_byte(cell_check_sum)
        row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
        return row_check_sum

    def write_primary_key(self, primary_key, row_check_sum):
        self.write_tag(TAG_ROW_PK)
        for pk in primary_key:
            row_check_sum = self.write_primary_key_column(pk[0], pk[1], row_check_sum)
        return row_check_sum

    def write_columns(self, columns, row_check_sum):
        if columns is not None and len(columns) != 0:
            self.write_tag(TAG_ROW_DATA)
            for column in columns:
                if len(column) == 2:
                    row_check_sum = self.write_column(column[0], column[1], None, row_check_sum)
                elif len(column) == 3:
                    row_check_sum = self.write_column(column[0], column[1], column[2], row_check_sum)
        return row_check_sum

    def write_update_columns(self, attribute_columns, row_check_sum):
        if len(attribute_columns) != 0:
            self.write_tag(TAG_ROW_DATA)
            for update_type in list(attribute_columns.keys()):
                columns = attribute_columns[update_type]
                for column in columns:
                    if isinstance(column, six.text_type) or isinstance(column, six.binary_type):
                        row_check_sum = self.write_update_column(update_type, column, None, row_check_sum)
                    elif len(column) == 2:
                        row_check_sum = self.write_update_column(update_type, column[0], (column[1], None), row_check_sum)
                    elif len(column) == 3:
                        row_check_sum = self.write_update_column(update_type, column[0], (column[1], column[2]), row_check_sum)
                    else:
                        raise OTSClientError("Unsupported column format: " + str(column))
        return row_check_sum

    def write_delete_marker(self, row_checksum):
        self.write_tag(TAG_DELETE_ROW_MARKER)
        return PlainBufferCrc8.crc_int8(row_checksum, 1);

    def write_row_checksum(self, row_checksum):
        self.write_tag(TAG_ROW_CHECKSUM)                                                                                  
        self.output_stream.write_raw_byte(row_checksum)

