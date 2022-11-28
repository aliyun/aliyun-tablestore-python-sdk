from tablestore.metadata import *
from .dataprotocol.DataType import *
import sys
class flat_buffer_decoder(object):
    @staticmethod
    def byte_to_str_decode(bt):
        if sys.version_info[0] == 2:
            return bt
        else:
            return bt.decode('UTF-8')

    @staticmethod
    def gen_meta_column(col_val,col_tp):
        ret = []
        col_val_decode_func_list = [None]
        col_val_decode_len_func_list = [None]

        col_val_decode_func_list.append(col_val.LongValues)
        col_val_decode_func_list.append(col_val.BoolValues)
        col_val_decode_func_list.append(col_val.DoubleValues)
        col_val_decode_func_list.append(col_val.StringValues)
        col_val_decode_func_list.append(col_val.BinaryValues)

        col_val_decode_len_func_list.append(col_val.LongValuesLength)
        col_val_decode_len_func_list.append(col_val.BoolValuesLength)
        col_val_decode_len_func_list.append(col_val.DoubleValuesLength)
        col_val_decode_len_func_list.append(col_val.StringValuesLength)
        col_val_decode_len_func_list.append(col_val.BinaryValuesLength)
            
        if col_tp == DataType.STRING_RLE:
            rle_string_obj = col_val.RleStringValues()
            for i in range(rle_string_obj.IndexMappingLength()):
                data_idx =  rle_string_obj.IndexMapping(i)
                #print(i,rle_string_obj.Array(data_idx).decode('UTF-8'))
                ret.append(rle_string_obj.Array(data_idx).decode('UTF-8'))  
            return ret
        
        for i in range(col_val_decode_len_func_list[col_tp]()):
            if col_val.IsNullvalues(i):
                ret.append(None)
                continue
            decode_val = col_val_decode_func_list[col_tp](i)
            if col_tp == DataType.BINARY:
                byte_list = []
                for j in range(decode_val.ValueLength()):
                    byte_list.append(decode_val.Value(j))
                decode_val = bytearray(byte_list)
            if col_tp == DataType.STRING:
                decode_val = flat_buffer_decoder.byte_to_str_decode(decode_val)
            ret.append(decode_val)
        return ret
        
    @staticmethod
    def format_flat_buffer_columns(columns):
        import collections
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
        import sys
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
        