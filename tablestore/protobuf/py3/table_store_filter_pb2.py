# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: table_store_filter.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x18table_store_filter.proto\x12\x1e\x63om.aliyun.tablestore.protocol\"\xc0\x01\n\x17SingleColumnValueFilter\x12\x42\n\ncomparator\x18\x01 \x02(\x0e\x32..com.aliyun.tablestore.protocol.ComparatorType\x12\x13\n\x0b\x63olumn_name\x18\x02 \x02(\t\x12\x14\n\x0c\x63olumn_value\x18\x03 \x02(\x0c\x12\x19\n\x11\x66ilter_if_missing\x18\x04 \x02(\x08\x12\x1b\n\x13latest_version_only\x18\x05 \x02(\x08\"\x9e\x01\n\x1a\x43ompositeColumnValueFilter\x12\x43\n\ncombinator\x18\x01 \x02(\x0e\x32/.com.aliyun.tablestore.protocol.LogicalOperator\x12;\n\x0bsub_filters\x18\x02 \x03(\x0b\x32&.com.aliyun.tablestore.protocol.Filter\"7\n\x16\x43olumnPaginationFilter\x12\x0e\n\x06offset\x18\x01 \x02(\x05\x12\r\n\x05limit\x18\x02 \x02(\x05\"R\n\x06\x46ilter\x12\x38\n\x04type\x18\x01 \x02(\x0e\x32*.com.aliyun.tablestore.protocol.FilterType\x12\x0e\n\x06\x66ilter\x18\x02 \x02(\x0c*a\n\nFilterType\x12\x1a\n\x16\x46T_SINGLE_COLUMN_VALUE\x10\x01\x12\x1d\n\x19\x46T_COMPOSITE_COLUMN_VALUE\x10\x02\x12\x18\n\x14\x46T_COLUMN_PAGINATION\x10\x03*\x80\x01\n\x0e\x43omparatorType\x12\x0c\n\x08\x43T_EQUAL\x10\x01\x12\x10\n\x0c\x43T_NOT_EQUAL\x10\x02\x12\x13\n\x0f\x43T_GREATER_THAN\x10\x03\x12\x14\n\x10\x43T_GREATER_EQUAL\x10\x04\x12\x10\n\x0c\x43T_LESS_THAN\x10\x05\x12\x11\n\rCT_LESS_EQUAL\x10\x06*4\n\x0fLogicalOperator\x12\n\n\x06LO_NOT\x10\x01\x12\n\n\x06LO_AND\x10\x02\x12\t\n\x05LO_OR\x10\x03')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'table_store_filter_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_FILTERTYPE']._serialized_start=557
  _globals['_FILTERTYPE']._serialized_end=654
  _globals['_COMPARATORTYPE']._serialized_start=657
  _globals['_COMPARATORTYPE']._serialized_end=785
  _globals['_LOGICALOPERATOR']._serialized_start=787
  _globals['_LOGICALOPERATOR']._serialized_end=839
  _globals['_SINGLECOLUMNVALUEFILTER']._serialized_start=61
  _globals['_SINGLECOLUMNVALUEFILTER']._serialized_end=253
  _globals['_COMPOSITECOLUMNVALUEFILTER']._serialized_start=256
  _globals['_COMPOSITECOLUMNVALUEFILTER']._serialized_end=414
  _globals['_COLUMNPAGINATIONFILTER']._serialized_start=416
  _globals['_COLUMNPAGINATIONFILTER']._serialized_end=471
  _globals['_FILTER']._serialized_start=473
  _globals['_FILTER']._serialized_end=555
# @@protoc_insertion_point(module_scope)
