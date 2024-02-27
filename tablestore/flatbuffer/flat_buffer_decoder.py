from tablestore.metadata import *
from .dataprotocol.DataType import *
import sys
import collections
from tablestore.flatbuffer.dataprotocol.ColumnValues import *
class flat_buffer_decoder(object):
    @staticmethod
    def byte_to_str_decode(bt):
        if sys.version_info[0] == 2:
            return bt
        else:
            return bt.decode('UTF-8')

    @staticmethod
    def gen_meta_column(col_val,col_tp):
        upackedVal = ColumnValuesT.InitFromObj(col_val)
        values = get_column_val_by_tp(upackedVal,col_tp)
        if col_tp == DataType.STRING_RLE:
            ret = []
            for i in range(len(values.indexMapping)):
                ret.append(values.array[values.indexMapping[i]].decode('UTF-8'))  
            return ret
        if col_tp == DataType.BINARY:
            byte_list = []
            for i in range(len(values)):
                byte_list.append(values[i].value.tobytes())
            values = byte_list
        elif col_tp == DataType.STRING:
            string_list = []
            for i in range(len(values)):
                string_list.append(flat_buffer_decoder.byte_to_str_decode(values[i]))
            values = string_list
        
        if len(upackedVal.isNullvalues) != len(values):
            raise ValueError("the length of unpacked values not equal to null map")

        return [None if is_null else val for is_null, val in zip(upackedVal.isNullvalues, values)]

        
    @staticmethod
    def format_flat_buffer_columns(columns):
        columns_meta = collections.defaultdict(list)
        for i in range(columns.ColumnsLength()):
            column = columns.Columns(i) 
            col_name = flat_buffer_decoder.byte_to_str_decode(column.ColumnName())
            col_tp = column.ColumnType()
            col_val = column.ColumnValue()
            columns_meta[col_name] = flat_buffer_decoder.gen_meta_column(col_val,col_tp)
        return columns_meta
    
    @staticmethod
    def columns_to_rows(columns_meta):
        res_list = []
        column_len = sys.maxsize
        for key in columns_meta:
            column_len = min(len(columns_meta[key]),column_len)
        for i in range(column_len):
            tup = []
            for key in columns_meta:
                tup.append((key,columns_meta[key][i]))
            row =Row(primary_key = [],attribute_columns=tup)
            res_list.append(row)
        return res_list

def get_column_val_by_tp(upackedVal,tp):
    if tp == DataType.NONE:
        return upackedVal.isNullvalues
    if tp == DataType.LONG:
        return upackedVal.longValues
    if tp == DataType.BOOLEAN:
        return upackedVal.boolValues
    if tp == DataType.DOUBLE:
        return upackedVal.doubleValues
    if tp == DataType.STRING:
        return upackedVal.stringValues
    if tp == DataType.BINARY:
        return upackedVal.binaryValues
    if tp == DataType.STRING_RLE:
        return upackedVal.rleStringValues
    return None