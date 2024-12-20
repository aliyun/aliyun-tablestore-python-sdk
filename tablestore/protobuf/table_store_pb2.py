# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: table_store.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11table_store.proto\x12\x1e\x63om.aliyun.tablestore.protocol\"&\n\x05\x45rror\x12\x0c\n\x04\x63ode\x18\x01 \x02(\t\x12\x0f\n\x07message\x18\x02 \x01(\t\"\xa0\x01\n\x10PrimaryKeySchema\x12\x0c\n\x04name\x18\x01 \x02(\t\x12<\n\x04type\x18\x02 \x02(\x0e\x32..com.aliyun.tablestore.protocol.PrimaryKeyType\x12@\n\x06option\x18\x03 \x01(\x0e\x32\x30.com.aliyun.tablestore.protocol.PrimaryKeyOption\"d\n\x13\x44\x65\x66inedColumnSchema\x12\x0c\n\x04name\x18\x01 \x02(\t\x12?\n\x04type\x18\x02 \x02(\x0e\x32\x31.com.aliyun.tablestore.protocol.DefinedColumnType\"w\n\x0cTableOptions\x12\x14\n\x0ctime_to_live\x18\x01 \x01(\x05\x12\x14\n\x0cmax_versions\x18\x02 \x01(\x05\x12%\n\x1d\x64\x65viation_cell_version_in_sec\x18\x05 \x01(\x03\x12\x14\n\x0c\x61llow_update\x18\x06 \x01(\x08\"\xd1\x01\n\tIndexMeta\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x13\n\x0bprimary_key\x18\x02 \x03(\t\x12\x16\n\x0e\x64\x65\x66ined_column\x18\x03 \x03(\t\x12J\n\x11index_update_mode\x18\x04 \x02(\x0e\x32/.com.aliyun.tablestore.protocol.IndexUpdateMode\x12=\n\nindex_type\x18\x05 \x02(\x0e\x32).com.aliyun.tablestore.protocol.IndexType\"\xb3\x01\n\tTableMeta\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x45\n\x0bprimary_key\x18\x02 \x03(\x0b\x32\x30.com.aliyun.tablestore.protocol.PrimaryKeySchema\x12K\n\x0e\x64\x65\x66ined_column\x18\x03 \x03(\x0b\x32\x33.com.aliyun.tablestore.protocol.DefinedColumnSchema\"u\n\tCondition\x12N\n\rrow_existence\x18\x01 \x02(\x0e\x32\x37.com.aliyun.tablestore.protocol.RowExistenceExpectation\x12\x18\n\x10\x63olumn_condition\x18\x02 \x01(\x0c\"+\n\x0c\x43\x61pacityUnit\x12\x0c\n\x04read\x18\x01 \x01(\x05\x12\r\n\x05write\x18\x02 \x01(\x05\"\x98\x01\n\x19ReservedThroughputDetails\x12\x43\n\rcapacity_unit\x18\x01 \x02(\x0b\x32,.com.aliyun.tablestore.protocol.CapacityUnit\x12\x1a\n\x12last_increase_time\x18\x02 \x02(\x03\x12\x1a\n\x12last_decrease_time\x18\x03 \x01(\x03\"Y\n\x12ReservedThroughput\x12\x43\n\rcapacity_unit\x18\x01 \x02(\x0b\x32,.com.aliyun.tablestore.protocol.CapacityUnit\"W\n\x10\x43onsumedCapacity\x12\x43\n\rcapacity_unit\x18\x01 \x02(\x0b\x32,.com.aliyun.tablestore.protocol.CapacityUnit\"E\n\x13StreamSpecification\x12\x15\n\renable_stream\x18\x01 \x02(\x08\x12\x17\n\x0f\x65xpiration_time\x18\x02 \x01(\x05\"l\n\rStreamDetails\x12\x15\n\renable_stream\x18\x01 \x02(\x08\x12\x11\n\tstream_id\x18\x02 \x01(\t\x12\x17\n\x0f\x65xpiration_time\x18\x03 \x01(\x05\x12\x18\n\x10last_enable_time\x18\x04 \x01(\x03\"\xf3\x02\n\x12\x43reateTableRequest\x12=\n\ntable_meta\x18\x01 \x02(\x0b\x32).com.aliyun.tablestore.protocol.TableMeta\x12O\n\x13reserved_throughput\x18\x02 \x02(\x0b\x32\x32.com.aliyun.tablestore.protocol.ReservedThroughput\x12\x43\n\rtable_options\x18\x03 \x01(\x0b\x32,.com.aliyun.tablestore.protocol.TableOptions\x12H\n\x0bstream_spec\x18\x05 \x01(\x0b\x32\x33.com.aliyun.tablestore.protocol.StreamSpecification\x12>\n\x0bindex_metas\x18\x07 \x03(\x0b\x32).com.aliyun.tablestore.protocol.IndexMeta\"\x15\n\x13\x43reateTableResponse\"\x87\x01\n\x12\x43reateIndexRequest\x12\x17\n\x0fmain_table_name\x18\x01 \x02(\t\x12=\n\nindex_meta\x18\x02 \x02(\x0b\x32).com.aliyun.tablestore.protocol.IndexMeta\x12\x19\n\x11include_base_data\x18\x03 \x01(\x08\"\x15\n\x13\x43reateIndexResponse\"?\n\x10\x44ropIndexRequest\x12\x17\n\x0fmain_table_name\x18\x01 \x02(\t\x12\x12\n\nindex_name\x18\x02 \x02(\t\"\x13\n\x11\x44ropIndexResponse\"\x88\x02\n\x12UpdateTableRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12O\n\x13reserved_throughput\x18\x02 \x01(\x0b\x32\x32.com.aliyun.tablestore.protocol.ReservedThroughput\x12\x43\n\rtable_options\x18\x03 \x01(\x0b\x32,.com.aliyun.tablestore.protocol.TableOptions\x12H\n\x0bstream_spec\x18\x04 \x01(\x0b\x32\x33.com.aliyun.tablestore.protocol.StreamSpecification\"\x81\x02\n\x13UpdateTableResponse\x12^\n\x1breserved_throughput_details\x18\x01 \x02(\x0b\x32\x39.com.aliyun.tablestore.protocol.ReservedThroughputDetails\x12\x43\n\rtable_options\x18\x02 \x02(\x0b\x32,.com.aliyun.tablestore.protocol.TableOptions\x12\x45\n\x0estream_details\x18\x03 \x01(\x0b\x32-.com.aliyun.tablestore.protocol.StreamDetails\"*\n\x14\x44\x65scribeTableRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\"\x98\x03\n\x15\x44\x65scribeTableResponse\x12=\n\ntable_meta\x18\x01 \x02(\x0b\x32).com.aliyun.tablestore.protocol.TableMeta\x12^\n\x1breserved_throughput_details\x18\x02 \x02(\x0b\x32\x39.com.aliyun.tablestore.protocol.ReservedThroughputDetails\x12\x43\n\rtable_options\x18\x03 \x02(\x0b\x32,.com.aliyun.tablestore.protocol.TableOptions\x12\x45\n\x0estream_details\x18\x05 \x01(\x0b\x32-.com.aliyun.tablestore.protocol.StreamDetails\x12\x14\n\x0cshard_splits\x18\x06 \x03(\x0c\x12>\n\x0bindex_metas\x18\x08 \x03(\x0b\x32).com.aliyun.tablestore.protocol.IndexMeta\"\x12\n\x10ListTableRequest\"(\n\x11ListTableResponse\x12\x13\n\x0btable_names\x18\x01 \x03(\t\"(\n\x12\x44\x65leteTableRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\"\x15\n\x13\x44\x65leteTableResponse\"&\n\x10LoadTableRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\"\x13\n\x11LoadTableResponse\"(\n\x12UnloadTableRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\"\x15\n\x13UnloadTableResponse\"H\n\tTimeRange\x12\x12\n\nstart_time\x18\x01 \x01(\x03\x12\x10\n\x08\x65nd_time\x18\x02 \x01(\x03\x12\x15\n\rspecific_time\x18\x03 \x01(\x03\"m\n\rReturnContent\x12?\n\x0breturn_type\x18\x01 \x01(\x0e\x32*.com.aliyun.tablestore.protocol.ReturnType\x12\x1b\n\x13return_column_names\x18\x02 \x03(\t\"\x86\x02\n\rGetRowRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x13\n\x0bprimary_key\x18\x02 \x02(\x0c\x12\x16\n\x0e\x63olumns_to_get\x18\x03 \x03(\t\x12=\n\ntime_range\x18\x04 \x01(\x0b\x32).com.aliyun.tablestore.protocol.TimeRange\x12\x14\n\x0cmax_versions\x18\x05 \x01(\x05\x12\x0e\n\x06\x66ilter\x18\x07 \x01(\x0c\x12\x14\n\x0cstart_column\x18\x08 \x01(\t\x12\x12\n\nend_column\x18\t \x01(\t\x12\r\n\x05token\x18\n \x01(\x0c\x12\x16\n\x0etransaction_id\x18\x0b \x01(\t\"u\n\x0eGetRowResponse\x12\x42\n\x08\x63onsumed\x18\x01 \x02(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0b\n\x03row\x18\x02 \x02(\x0c\x12\x12\n\nnext_token\x18\x03 \x01(\x0c\"\xd7\x01\n\x10UpdateRowRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x12\n\nrow_change\x18\x02 \x02(\x0c\x12<\n\tcondition\x18\x03 \x02(\x0b\x32).com.aliyun.tablestore.protocol.Condition\x12\x45\n\x0ereturn_content\x18\x04 \x01(\x0b\x32-.com.aliyun.tablestore.protocol.ReturnContent\x12\x16\n\x0etransaction_id\x18\x05 \x01(\t\"d\n\x11UpdateRowResponse\x12\x42\n\x08\x63onsumed\x18\x01 \x02(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0b\n\x03row\x18\x02 \x01(\x0c\"\xcd\x01\n\rPutRowRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x0b\n\x03row\x18\x02 \x02(\x0c\x12<\n\tcondition\x18\x03 \x02(\x0b\x32).com.aliyun.tablestore.protocol.Condition\x12\x45\n\x0ereturn_content\x18\x04 \x01(\x0b\x32-.com.aliyun.tablestore.protocol.ReturnContent\x12\x16\n\x0etransaction_id\x18\x05 \x01(\t\"a\n\x0ePutRowResponse\x12\x42\n\x08\x63onsumed\x18\x01 \x02(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0b\n\x03row\x18\x02 \x01(\x0c\"\xd8\x01\n\x10\x44\x65leteRowRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x13\n\x0bprimary_key\x18\x02 \x02(\x0c\x12<\n\tcondition\x18\x03 \x02(\x0b\x32).com.aliyun.tablestore.protocol.Condition\x12\x45\n\x0ereturn_content\x18\x04 \x01(\x0b\x32-.com.aliyun.tablestore.protocol.ReturnContent\x12\x16\n\x0etransaction_id\x18\x05 \x01(\t\"d\n\x11\x44\x65leteRowResponse\x12\x42\n\x08\x63onsumed\x18\x01 \x02(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0b\n\x03row\x18\x02 \x01(\x0c\"\xfa\x01\n\x19TableInBatchGetRowRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x13\n\x0bprimary_key\x18\x02 \x03(\x0c\x12\r\n\x05token\x18\x03 \x03(\x0c\x12\x16\n\x0e\x63olumns_to_get\x18\x04 \x03(\t\x12=\n\ntime_range\x18\x05 \x01(\x0b\x32).com.aliyun.tablestore.protocol.TimeRange\x12\x14\n\x0cmax_versions\x18\x06 \x01(\x05\x12\x0e\n\x06\x66ilter\x18\x08 \x01(\x0c\x12\x14\n\x0cstart_column\x18\t \x01(\t\x12\x12\n\nend_column\x18\n \x01(\t\"_\n\x12\x42\x61tchGetRowRequest\x12I\n\x06tables\x18\x01 \x03(\x0b\x32\x39.com.aliyun.tablestore.protocol.TableInBatchGetRowRequest\"\xc4\x01\n\x18RowInBatchGetRowResponse\x12\r\n\x05is_ok\x18\x01 \x02(\x08\x12\x34\n\x05\x65rror\x18\x02 \x01(\x0b\x32%.com.aliyun.tablestore.protocol.Error\x12\x42\n\x08\x63onsumed\x18\x03 \x01(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0b\n\x03row\x18\x04 \x01(\x0c\x12\x12\n\nnext_token\x18\x05 \x01(\x0c\"x\n\x1aTableInBatchGetRowResponse\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x46\n\x04rows\x18\x02 \x03(\x0b\x32\x38.com.aliyun.tablestore.protocol.RowInBatchGetRowResponse\"a\n\x13\x42\x61tchGetRowResponse\x12J\n\x06tables\x18\x01 \x03(\x0b\x32:.com.aliyun.tablestore.protocol.TableInBatchGetRowResponse\"\xf1\x01\n\x19RowInBatchWriteRowRequest\x12;\n\x04type\x18\x01 \x02(\x0e\x32-.com.aliyun.tablestore.protocol.OperationType\x12\x12\n\nrow_change\x18\x02 \x02(\x0c\x12<\n\tcondition\x18\x03 \x02(\x0b\x32).com.aliyun.tablestore.protocol.Condition\x12\x45\n\x0ereturn_content\x18\x04 \x01(\x0b\x32-.com.aliyun.tablestore.protocol.ReturnContent\"z\n\x1bTableInBatchWriteRowRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12G\n\x04rows\x18\x02 \x03(\x0b\x32\x39.com.aliyun.tablestore.protocol.RowInBatchWriteRowRequest\"{\n\x14\x42\x61tchWriteRowRequest\x12K\n\x06tables\x18\x01 \x03(\x0b\x32;.com.aliyun.tablestore.protocol.TableInBatchWriteRowRequest\x12\x16\n\x0etransaction_id\x18\x02 \x01(\t\"\xb2\x01\n\x1aRowInBatchWriteRowResponse\x12\r\n\x05is_ok\x18\x01 \x02(\x08\x12\x34\n\x05\x65rror\x18\x02 \x01(\x0b\x32%.com.aliyun.tablestore.protocol.Error\x12\x42\n\x08\x63onsumed\x18\x03 \x01(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0b\n\x03row\x18\x04 \x01(\x0c\"|\n\x1cTableInBatchWriteRowResponse\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12H\n\x04rows\x18\x02 \x03(\x0b\x32:.com.aliyun.tablestore.protocol.RowInBatchWriteRowResponse\"e\n\x15\x42\x61tchWriteRowResponse\x12L\n\x06tables\x18\x01 \x03(\x0b\x32<.com.aliyun.tablestore.protocol.TableInBatchWriteRowResponse\"\x88\x03\n\x0fGetRangeRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12<\n\tdirection\x18\x02 \x02(\x0e\x32).com.aliyun.tablestore.protocol.Direction\x12\x16\n\x0e\x63olumns_to_get\x18\x03 \x03(\t\x12=\n\ntime_range\x18\x04 \x01(\x0b\x32).com.aliyun.tablestore.protocol.TimeRange\x12\x14\n\x0cmax_versions\x18\x05 \x01(\x05\x12\r\n\x05limit\x18\x06 \x01(\x05\x12#\n\x1binclusive_start_primary_key\x18\x07 \x02(\x0c\x12!\n\x19\x65xclusive_end_primary_key\x18\x08 \x02(\x0c\x12\x0e\n\x06\x66ilter\x18\n \x01(\x0c\x12\x14\n\x0cstart_column\x18\x0b \x01(\t\x12\x12\n\nend_column\x18\x0c \x01(\t\x12\r\n\x05token\x18\r \x01(\x0c\x12\x16\n\x0etransaction_id\x18\x0e \x01(\t\"\x98\x01\n\x10GetRangeResponse\x12\x42\n\x08\x63onsumed\x18\x01 \x02(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12\x0c\n\x04rows\x18\x02 \x02(\x0c\x12\x1e\n\x16next_start_primary_key\x18\x03 \x01(\x0c\x12\x12\n\nnext_token\x18\x04 \x01(\x0c\"?\n\x1cStartLocalTransactionRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x0b\n\x03key\x18\x02 \x02(\x0c\"7\n\x1dStartLocalTransactionResponse\x12\x16\n\x0etransaction_id\x18\x01 \x02(\t\"2\n\x18\x43ommitTransactionRequest\x12\x16\n\x0etransaction_id\x18\x01 \x02(\t\"\x1b\n\x19\x43ommitTransactionResponse\"1\n\x17\x41\x62ortTransactionRequest\x12\x16\n\x0etransaction_id\x18\x01 \x02(\t\"\x1a\n\x18\x41\x62ortTransactionResponse\"\'\n\x11ListStreamRequest\x12\x12\n\ntable_name\x18\x01 \x01(\t\"F\n\x06Stream\x12\x11\n\tstream_id\x18\x01 \x02(\t\x12\x12\n\ntable_name\x18\x02 \x02(\t\x12\x15\n\rcreation_time\x18\x03 \x02(\x03\"M\n\x12ListStreamResponse\x12\x37\n\x07streams\x18\x01 \x03(\x0b\x32&.com.aliyun.tablestore.protocol.Stream\"M\n\x0bStreamShard\x12\x10\n\x08shard_id\x18\x01 \x02(\t\x12\x11\n\tparent_id\x18\x02 \x01(\t\x12\x19\n\x11parent_sibling_id\x18\x03 \x01(\t\"a\n\x15\x44\x65scribeStreamRequest\x12\x11\n\tstream_id\x18\x01 \x02(\t\x12 \n\x18inclusive_start_shard_id\x18\x02 \x01(\t\x12\x13\n\x0bshard_limit\x18\x03 \x01(\x05\"\x88\x02\n\x16\x44\x65scribeStreamResponse\x12\x11\n\tstream_id\x18\x01 \x02(\t\x12\x17\n\x0f\x65xpiration_time\x18\x02 \x02(\x05\x12\x12\n\ntable_name\x18\x03 \x02(\t\x12\x15\n\rcreation_time\x18\x04 \x02(\x03\x12\x43\n\rstream_status\x18\x05 \x02(\x0e\x32,.com.aliyun.tablestore.protocol.StreamStatus\x12;\n\x06shards\x18\x06 \x03(\x0b\x32+.com.aliyun.tablestore.protocol.StreamShard\x12\x15\n\rnext_shard_id\x18\x07 \x01(\t\">\n\x17GetShardIteratorRequest\x12\x11\n\tstream_id\x18\x01 \x02(\t\x12\x10\n\x08shard_id\x18\x02 \x02(\t\"2\n\x18GetShardIteratorResponse\x12\x16\n\x0eshard_iterator\x18\x01 \x02(\t\"?\n\x16GetStreamRecordRequest\x12\x16\n\x0eshard_iterator\x18\x01 \x02(\t\x12\r\n\x05limit\x18\x02 \x01(\x05\"\xf5\x01\n\x17GetStreamRecordResponse\x12\\\n\x0estream_records\x18\x01 \x03(\x0b\x32\x44.com.aliyun.tablestore.protocol.GetStreamRecordResponse.StreamRecord\x12\x1b\n\x13next_shard_iterator\x18\x02 \x01(\t\x1a_\n\x0cStreamRecord\x12?\n\x0b\x61\x63tion_type\x18\x01 \x02(\x0e\x32*.com.aliyun.tablestore.protocol.ActionType\x12\x0e\n\x06record\x18\x02 \x02(\x0c\"j\n\x1f\x43omputeSplitPointsBySizeRequest\x12\x12\n\ntable_name\x18\x01 \x02(\t\x12\x12\n\nsplit_size\x18\x02 \x02(\x03\x12\x1f\n\x17split_size_unit_in_byte\x18\x03 \x01(\x03\"\xd4\x02\n ComputeSplitPointsBySizeResponse\x12\x42\n\x08\x63onsumed\x18\x01 \x02(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12@\n\x06schema\x18\x02 \x03(\x0b\x32\x30.com.aliyun.tablestore.protocol.PrimaryKeySchema\x12\x14\n\x0csplit_points\x18\x03 \x03(\x0c\x12\x61\n\tlocations\x18\x04 \x03(\x0b\x32N.com.aliyun.tablestore.protocol.ComputeSplitPointsBySizeResponse.SplitLocation\x1a\x31\n\rSplitLocation\x12\x10\n\x08location\x18\x01 \x02(\t\x12\x0e\n\x06repeat\x18\x02 \x02(\x12\"d\n\x0fSQLQueryRequest\x12\r\n\x05query\x18\x01 \x02(\t\x12\x42\n\x07version\x18\x02 \x01(\x0e\x32\x31.com.aliyun.tablestore.protocol.SQLPayloadVersion\"\xdc\x01\n\x15TableConsumedCapacity\x12\x12\n\ntable_name\x18\x01 \x01(\t\x12\x42\n\x08\x63onsumed\x18\x02 \x01(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12O\n\x13reserved_throughput\x18\x03 \x01(\x0b\x32\x32.com.aliyun.tablestore.protocol.ReservedThroughput\x12\x1a\n\x12is_timeseries_meta\x18\x04 \x01(\x08\"\xd5\x01\n\x16SearchConsumedCapacity\x12\x12\n\ntable_name\x18\x01 \x01(\t\x12\x12\n\nindex_name\x18\x02 \x01(\t\x12\x42\n\x08\x63onsumed\x18\x03 \x01(\x0b\x32\x30.com.aliyun.tablestore.protocol.ConsumedCapacity\x12O\n\x13reserved_throughput\x18\x04 \x01(\x0b\x32\x32.com.aliyun.tablestore.protocol.ReservedThroughput\"\xbe\x02\n\x10SQLQueryResponse\x12G\n\x08\x63onsumes\x18\x01 \x03(\x0b\x32\x35.com.aliyun.tablestore.protocol.TableConsumedCapacity\x12\x0c\n\x04rows\x18\x02 \x01(\x0c\x12\x42\n\x07version\x18\x03 \x01(\x0e\x32\x31.com.aliyun.tablestore.protocol.SQLPayloadVersion\x12>\n\x04type\x18\x04 \x01(\x0e\x32\x30.com.aliyun.tablestore.protocol.SQLStatementType\x12O\n\x0fsearch_consumes\x18\x05 \x03(\x0b\x32\x36.com.aliyun.tablestore.protocol.SearchConsumedCapacity*5\n\x0ePrimaryKeyType\x12\x0b\n\x07INTEGER\x10\x01\x12\n\n\x06STRING\x10\x02\x12\n\n\x06\x42INARY\x10\x03*c\n\x11\x44\x65\x66inedColumnType\x12\x0f\n\x0b\x44\x43T_INTEGER\x10\x01\x12\x0e\n\nDCT_DOUBLE\x10\x02\x12\x0f\n\x0b\x44\x43T_BOOLEAN\x10\x03\x12\x0e\n\nDCT_STRING\x10\x04\x12\x0c\n\x08\x44\x43T_BLOB\x10\x07*&\n\x10PrimaryKeyOption\x12\x12\n\x0e\x41UTO_INCREMENT\x10\x01*:\n\x0fIndexUpdateMode\x12\x13\n\x0fIUM_ASYNC_INDEX\x10\x00\x12\x12\n\x0eIUM_SYNC_INDEX\x10\x01*4\n\tIndexType\x12\x13\n\x0fIT_GLOBAL_INDEX\x10\x00\x12\x12\n\x0eIT_LOCAL_INDEX\x10\x01*M\n\x17RowExistenceExpectation\x12\n\n\x06IGNORE\x10\x00\x12\x10\n\x0c\x45XPECT_EXIST\x10\x01\x12\x14\n\x10\x45XPECT_NOT_EXIST\x10\x02*9\n\nReturnType\x12\x0b\n\x07RT_NONE\x10\x00\x12\t\n\x05RT_PK\x10\x01\x12\x13\n\x0fRT_AFTER_MODIFY\x10\x02*0\n\rOperationType\x12\x07\n\x03PUT\x10\x01\x12\n\n\x06UPDATE\x10\x02\x12\n\n\x06\x44\x45LETE\x10\x03*&\n\tDirection\x12\x0b\n\x07\x46ORWARD\x10\x00\x12\x0c\n\x08\x42\x41\x43KWARD\x10\x01*6\n\x0cStreamStatus\x12\x13\n\x0fSTREAM_ENABLING\x10\x01\x12\x11\n\rSTREAM_ACTIVE\x10\x02*9\n\nActionType\x12\x0b\n\x07PUT_ROW\x10\x01\x12\x0e\n\nUPDATE_ROW\x10\x02\x12\x0e\n\nDELETE_ROW\x10\x03*?\n\x11SQLPayloadVersion\x12\x14\n\x10SQL_PLAIN_BUFFER\x10\x01\x12\x14\n\x10SQL_FLAT_BUFFERS\x10\x02*\x8d\x01\n\x10SQLStatementType\x12\x0e\n\nSQL_SELECT\x10\x01\x12\x14\n\x10SQL_CREATE_TABLE\x10\x02\x12\x12\n\x0eSQL_SHOW_TABLE\x10\x03\x12\x16\n\x12SQL_DESCRIBE_TABLE\x10\x04\x12\x12\n\x0eSQL_DROP_TABLE\x10\x05\x12\x13\n\x0fSQL_ALTER_TABLE\x10\x06')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'table_store_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_PRIMARYKEYTYPE']._serialized_start=9833
  _globals['_PRIMARYKEYTYPE']._serialized_end=9886
  _globals['_DEFINEDCOLUMNTYPE']._serialized_start=9888
  _globals['_DEFINEDCOLUMNTYPE']._serialized_end=9987
  _globals['_PRIMARYKEYOPTION']._serialized_start=9989
  _globals['_PRIMARYKEYOPTION']._serialized_end=10027
  _globals['_INDEXUPDATEMODE']._serialized_start=10029
  _globals['_INDEXUPDATEMODE']._serialized_end=10087
  _globals['_INDEXTYPE']._serialized_start=10089
  _globals['_INDEXTYPE']._serialized_end=10141
  _globals['_ROWEXISTENCEEXPECTATION']._serialized_start=10143
  _globals['_ROWEXISTENCEEXPECTATION']._serialized_end=10220
  _globals['_RETURNTYPE']._serialized_start=10222
  _globals['_RETURNTYPE']._serialized_end=10279
  _globals['_OPERATIONTYPE']._serialized_start=10281
  _globals['_OPERATIONTYPE']._serialized_end=10329
  _globals['_DIRECTION']._serialized_start=10331
  _globals['_DIRECTION']._serialized_end=10369
  _globals['_STREAMSTATUS']._serialized_start=10371
  _globals['_STREAMSTATUS']._serialized_end=10425
  _globals['_ACTIONTYPE']._serialized_start=10427
  _globals['_ACTIONTYPE']._serialized_end=10484
  _globals['_SQLPAYLOADVERSION']._serialized_start=10486
  _globals['_SQLPAYLOADVERSION']._serialized_end=10549
  _globals['_SQLSTATEMENTTYPE']._serialized_start=10552
  _globals['_SQLSTATEMENTTYPE']._serialized_end=10693
  _globals['_ERROR']._serialized_start=53
  _globals['_ERROR']._serialized_end=91
  _globals['_PRIMARYKEYSCHEMA']._serialized_start=94
  _globals['_PRIMARYKEYSCHEMA']._serialized_end=254
  _globals['_DEFINEDCOLUMNSCHEMA']._serialized_start=256
  _globals['_DEFINEDCOLUMNSCHEMA']._serialized_end=356
  _globals['_TABLEOPTIONS']._serialized_start=358
  _globals['_TABLEOPTIONS']._serialized_end=477
  _globals['_INDEXMETA']._serialized_start=480
  _globals['_INDEXMETA']._serialized_end=689
  _globals['_TABLEMETA']._serialized_start=692
  _globals['_TABLEMETA']._serialized_end=871
  _globals['_CONDITION']._serialized_start=873
  _globals['_CONDITION']._serialized_end=990
  _globals['_CAPACITYUNIT']._serialized_start=992
  _globals['_CAPACITYUNIT']._serialized_end=1035
  _globals['_RESERVEDTHROUGHPUTDETAILS']._serialized_start=1038
  _globals['_RESERVEDTHROUGHPUTDETAILS']._serialized_end=1190
  _globals['_RESERVEDTHROUGHPUT']._serialized_start=1192
  _globals['_RESERVEDTHROUGHPUT']._serialized_end=1281
  _globals['_CONSUMEDCAPACITY']._serialized_start=1283
  _globals['_CONSUMEDCAPACITY']._serialized_end=1370
  _globals['_STREAMSPECIFICATION']._serialized_start=1372
  _globals['_STREAMSPECIFICATION']._serialized_end=1441
  _globals['_STREAMDETAILS']._serialized_start=1443
  _globals['_STREAMDETAILS']._serialized_end=1551
  _globals['_CREATETABLEREQUEST']._serialized_start=1554
  _globals['_CREATETABLEREQUEST']._serialized_end=1925
  _globals['_CREATETABLERESPONSE']._serialized_start=1927
  _globals['_CREATETABLERESPONSE']._serialized_end=1948
  _globals['_CREATEINDEXREQUEST']._serialized_start=1951
  _globals['_CREATEINDEXREQUEST']._serialized_end=2086
  _globals['_CREATEINDEXRESPONSE']._serialized_start=2088
  _globals['_CREATEINDEXRESPONSE']._serialized_end=2109
  _globals['_DROPINDEXREQUEST']._serialized_start=2111
  _globals['_DROPINDEXREQUEST']._serialized_end=2174
  _globals['_DROPINDEXRESPONSE']._serialized_start=2176
  _globals['_DROPINDEXRESPONSE']._serialized_end=2195
  _globals['_UPDATETABLEREQUEST']._serialized_start=2198
  _globals['_UPDATETABLEREQUEST']._serialized_end=2462
  _globals['_UPDATETABLERESPONSE']._serialized_start=2465
  _globals['_UPDATETABLERESPONSE']._serialized_end=2722
  _globals['_DESCRIBETABLEREQUEST']._serialized_start=2724
  _globals['_DESCRIBETABLEREQUEST']._serialized_end=2766
  _globals['_DESCRIBETABLERESPONSE']._serialized_start=2769
  _globals['_DESCRIBETABLERESPONSE']._serialized_end=3177
  _globals['_LISTTABLEREQUEST']._serialized_start=3179
  _globals['_LISTTABLEREQUEST']._serialized_end=3197
  _globals['_LISTTABLERESPONSE']._serialized_start=3199
  _globals['_LISTTABLERESPONSE']._serialized_end=3239
  _globals['_DELETETABLEREQUEST']._serialized_start=3241
  _globals['_DELETETABLEREQUEST']._serialized_end=3281
  _globals['_DELETETABLERESPONSE']._serialized_start=3283
  _globals['_DELETETABLERESPONSE']._serialized_end=3304
  _globals['_LOADTABLEREQUEST']._serialized_start=3306
  _globals['_LOADTABLEREQUEST']._serialized_end=3344
  _globals['_LOADTABLERESPONSE']._serialized_start=3346
  _globals['_LOADTABLERESPONSE']._serialized_end=3365
  _globals['_UNLOADTABLEREQUEST']._serialized_start=3367
  _globals['_UNLOADTABLEREQUEST']._serialized_end=3407
  _globals['_UNLOADTABLERESPONSE']._serialized_start=3409
  _globals['_UNLOADTABLERESPONSE']._serialized_end=3430
  _globals['_TIMERANGE']._serialized_start=3432
  _globals['_TIMERANGE']._serialized_end=3504
  _globals['_RETURNCONTENT']._serialized_start=3506
  _globals['_RETURNCONTENT']._serialized_end=3615
  _globals['_GETROWREQUEST']._serialized_start=3618
  _globals['_GETROWREQUEST']._serialized_end=3880
  _globals['_GETROWRESPONSE']._serialized_start=3882
  _globals['_GETROWRESPONSE']._serialized_end=3999
  _globals['_UPDATEROWREQUEST']._serialized_start=4002
  _globals['_UPDATEROWREQUEST']._serialized_end=4217
  _globals['_UPDATEROWRESPONSE']._serialized_start=4219
  _globals['_UPDATEROWRESPONSE']._serialized_end=4319
  _globals['_PUTROWREQUEST']._serialized_start=4322
  _globals['_PUTROWREQUEST']._serialized_end=4527
  _globals['_PUTROWRESPONSE']._serialized_start=4529
  _globals['_PUTROWRESPONSE']._serialized_end=4626
  _globals['_DELETEROWREQUEST']._serialized_start=4629
  _globals['_DELETEROWREQUEST']._serialized_end=4845
  _globals['_DELETEROWRESPONSE']._serialized_start=4847
  _globals['_DELETEROWRESPONSE']._serialized_end=4947
  _globals['_TABLEINBATCHGETROWREQUEST']._serialized_start=4950
  _globals['_TABLEINBATCHGETROWREQUEST']._serialized_end=5200
  _globals['_BATCHGETROWREQUEST']._serialized_start=5202
  _globals['_BATCHGETROWREQUEST']._serialized_end=5297
  _globals['_ROWINBATCHGETROWRESPONSE']._serialized_start=5300
  _globals['_ROWINBATCHGETROWRESPONSE']._serialized_end=5496
  _globals['_TABLEINBATCHGETROWRESPONSE']._serialized_start=5498
  _globals['_TABLEINBATCHGETROWRESPONSE']._serialized_end=5618
  _globals['_BATCHGETROWRESPONSE']._serialized_start=5620
  _globals['_BATCHGETROWRESPONSE']._serialized_end=5717
  _globals['_ROWINBATCHWRITEROWREQUEST']._serialized_start=5720
  _globals['_ROWINBATCHWRITEROWREQUEST']._serialized_end=5961
  _globals['_TABLEINBATCHWRITEROWREQUEST']._serialized_start=5963
  _globals['_TABLEINBATCHWRITEROWREQUEST']._serialized_end=6085
  _globals['_BATCHWRITEROWREQUEST']._serialized_start=6087
  _globals['_BATCHWRITEROWREQUEST']._serialized_end=6210
  _globals['_ROWINBATCHWRITEROWRESPONSE']._serialized_start=6213
  _globals['_ROWINBATCHWRITEROWRESPONSE']._serialized_end=6391
  _globals['_TABLEINBATCHWRITEROWRESPONSE']._serialized_start=6393
  _globals['_TABLEINBATCHWRITEROWRESPONSE']._serialized_end=6517
  _globals['_BATCHWRITEROWRESPONSE']._serialized_start=6519
  _globals['_BATCHWRITEROWRESPONSE']._serialized_end=6620
  _globals['_GETRANGEREQUEST']._serialized_start=6623
  _globals['_GETRANGEREQUEST']._serialized_end=7015
  _globals['_GETRANGERESPONSE']._serialized_start=7018
  _globals['_GETRANGERESPONSE']._serialized_end=7170
  _globals['_STARTLOCALTRANSACTIONREQUEST']._serialized_start=7172
  _globals['_STARTLOCALTRANSACTIONREQUEST']._serialized_end=7235
  _globals['_STARTLOCALTRANSACTIONRESPONSE']._serialized_start=7237
  _globals['_STARTLOCALTRANSACTIONRESPONSE']._serialized_end=7292
  _globals['_COMMITTRANSACTIONREQUEST']._serialized_start=7294
  _globals['_COMMITTRANSACTIONREQUEST']._serialized_end=7344
  _globals['_COMMITTRANSACTIONRESPONSE']._serialized_start=7346
  _globals['_COMMITTRANSACTIONRESPONSE']._serialized_end=7373
  _globals['_ABORTTRANSACTIONREQUEST']._serialized_start=7375
  _globals['_ABORTTRANSACTIONREQUEST']._serialized_end=7424
  _globals['_ABORTTRANSACTIONRESPONSE']._serialized_start=7426
  _globals['_ABORTTRANSACTIONRESPONSE']._serialized_end=7452
  _globals['_LISTSTREAMREQUEST']._serialized_start=7454
  _globals['_LISTSTREAMREQUEST']._serialized_end=7493
  _globals['_STREAM']._serialized_start=7495
  _globals['_STREAM']._serialized_end=7565
  _globals['_LISTSTREAMRESPONSE']._serialized_start=7567
  _globals['_LISTSTREAMRESPONSE']._serialized_end=7644
  _globals['_STREAMSHARD']._serialized_start=7646
  _globals['_STREAMSHARD']._serialized_end=7723
  _globals['_DESCRIBESTREAMREQUEST']._serialized_start=7725
  _globals['_DESCRIBESTREAMREQUEST']._serialized_end=7822
  _globals['_DESCRIBESTREAMRESPONSE']._serialized_start=7825
  _globals['_DESCRIBESTREAMRESPONSE']._serialized_end=8089
  _globals['_GETSHARDITERATORREQUEST']._serialized_start=8091
  _globals['_GETSHARDITERATORREQUEST']._serialized_end=8153
  _globals['_GETSHARDITERATORRESPONSE']._serialized_start=8155
  _globals['_GETSHARDITERATORRESPONSE']._serialized_end=8205
  _globals['_GETSTREAMRECORDREQUEST']._serialized_start=8207
  _globals['_GETSTREAMRECORDREQUEST']._serialized_end=8270
  _globals['_GETSTREAMRECORDRESPONSE']._serialized_start=8273
  _globals['_GETSTREAMRECORDRESPONSE']._serialized_end=8518
  _globals['_GETSTREAMRECORDRESPONSE_STREAMRECORD']._serialized_start=8423
  _globals['_GETSTREAMRECORDRESPONSE_STREAMRECORD']._serialized_end=8518
  _globals['_COMPUTESPLITPOINTSBYSIZEREQUEST']._serialized_start=8520
  _globals['_COMPUTESPLITPOINTSBYSIZEREQUEST']._serialized_end=8626
  _globals['_COMPUTESPLITPOINTSBYSIZERESPONSE']._serialized_start=8629
  _globals['_COMPUTESPLITPOINTSBYSIZERESPONSE']._serialized_end=8969
  _globals['_COMPUTESPLITPOINTSBYSIZERESPONSE_SPLITLOCATION']._serialized_start=8920
  _globals['_COMPUTESPLITPOINTSBYSIZERESPONSE_SPLITLOCATION']._serialized_end=8969
  _globals['_SQLQUERYREQUEST']._serialized_start=8971
  _globals['_SQLQUERYREQUEST']._serialized_end=9071
  _globals['_TABLECONSUMEDCAPACITY']._serialized_start=9074
  _globals['_TABLECONSUMEDCAPACITY']._serialized_end=9294
  _globals['_SEARCHCONSUMEDCAPACITY']._serialized_start=9297
  _globals['_SEARCHCONSUMEDCAPACITY']._serialized_end=9510
  _globals['_SQLQUERYRESPONSE']._serialized_start=9513
  _globals['_SQLQUERYRESPONSE']._serialized_end=9831
# @@protoc_insertion_point(module_scope)