# -*- coding: utf8 -*-

import google.protobuf.text_format as text_format

from tablestore.metadata import *
from tablestore.aggregation import *
from tablestore.group_by import *
from tablestore.plainbuffer.plain_buffer_builder import *

import tablestore.protobuf.table_store_pb2 as pb
import tablestore.protobuf.table_store_filter_pb2 as filter_pb
import tablestore.protobuf.search_pb2 as search_pb

class OTSProtoBufferDecoder(object):

    def __init__(self, encoding):
        self.encoding = encoding

        self.api_decode_map = {
            'CreateTable'           : self._decode_create_table,
            'ListTable'             : self._decode_list_table,
            'DeleteTable'           : self._decode_delete_table,
            'DescribeTable'         : self._decode_describe_table,
            'UpdateTable'           : self._decode_update_table,
            'GetRow'                : self._decode_get_row,
            'PutRow'                : self._decode_put_row,
            'UpdateRow'             : self._decode_update_row,
            'DeleteRow'             : self._decode_delete_row,
            'BatchGetRow'           : self._decode_batch_get_row,
            'BatchWriteRow'         : self._decode_batch_write_row,
            'GetRange'              : self._decode_get_range,
            'ListSearchIndex'       : self._decode_list_search_index,
            'DeleteSearchIndex'     : self._decode_delete_search_index,
            'DescribeSearchIndex'   : self._decode_describe_search_index,
            'CreateSearchIndex'     : self._decode_create_search_index,
            'Search'                : self._decode_search,
            'ComputeSplits'         : self._decode_compute_splits,
            'ParallelScan'          : self._decode_parallel_scan,
            'CreateIndex'           : self._decode_create_index,
            'DropIndex'             : self._decode_delete_index,
            'StartLocalTransaction' : self._decode_start_local_transaction,
            'CommitTransaction'     : self._decode_commit_transaction,
            'AbortTransaction'      : self._decode_abort_transaction,
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

    def _parse_secondary_indexes(self, index_metas):
        secondary_indexes = []
        if index_metas:
            for secondary_index in index_metas:
                primary_key_names = [name for name in secondary_index.primary_key]
                defined_column_names = [name for name in secondary_index.defined_column]
                index_type = SecondaryIndexType.GLOBAL_INDEX if secondary_index.index_type == pb.IT_GLOBAL_INDEX else SecondaryIndexType.LOCAL_INDEX
                secondary_indexes.append(
                    SecondaryIndexMeta(
                        secondary_index.name,
                        primary_key_names,
                        defined_column_names,
                        index_type
                    )
                )
        return secondary_indexes

    def _get_defined_column_type(self, column_type):
        if column_type == pb.DCT_INTEGER:
            return 'INTEGER'
        elif column_type == pb.DCT_DOUBLE:
            return 'DOUBLE'
        elif column_type == pb.DCT_BOOLEAN:
            return 'BOOLEAN'
        elif column_type == pb.DCT_STRING:
            return 'STRING'
        elif column_type == pb.DCT_BLOB:
            return 'BINARY'
        else:
            raise OTSClientError(
                'Unknown defined column type: ' + str(column_type)
            )

    def _parse_defined_columns(self, defined_columns_proto):
        defined_columns = []

        for df in defined_columns_proto:
            defined_columns.append((df.name, self._get_defined_column_type(df.type)))

        return defined_columns

    def _decode_describe_table(self, body):
        proto = pb.DescribeTableResponse()
        proto.ParseFromString(body)

        table_meta = TableMeta(
            proto.table_meta.table_name,
            self._parse_schema_list(
                proto.table_meta.primary_key
            ),
            self._parse_defined_columns(
                proto.table_meta.defined_column
            )
        )

        reserved_throughput_details = self._parse_reserved_throughput_details(proto.reserved_throughput_details)
        table_options = self._parse_table_options(proto.table_options)
        secondary_indexes = self._parse_secondary_indexes(proto.index_metas)
        describe_table_response = DescribeTableResponse(table_meta, table_options, reserved_throughput_details, secondary_indexes)
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

    def _parse_index_info(self, proto):
        return (proto.table_name, proto.index_name)

    def _parse_field_type(self, proto):
        field_type = None
        if proto == search_pb.LONG:
            field_type = FieldType.LONG
        elif proto == search_pb.DOUBLE:
            field_type = FieldType.DOUBLE
        elif proto == search_pb.BOOLEAN:
            field_type = FieldType.BOOLEAN
        elif proto == search_pb.KEYWORD:
            field_type = FieldType.KEYWORD
        elif proto == search_pb.TEXT:
            field_type = FieldType.TEXT
        elif proto == search_pb.NESTED:
            field_type = FieldType.NESTED
        elif proto == search_pb.GEO_POINT:
            field_type = FieldType.GEOPOINT
        else:
            raise OTSClientError("Unknown field type: %d" % proto)

        return field_type

    def _parse_field_schema(self, proto):
        sub_field_schemas = []
        for sub_field_schema_proto in proto.field_schemas:
            sub_field_schemas.append(self._parse_field_schema(sub_field_schema_proto))

        field_schema = FieldSchema(
            proto.field_name, self._parse_field_type(proto.field_type),
            index=proto.index, store=proto.store, is_array=proto.is_array,
            enable_sort_and_agg=proto.enable_sort_and_agg,
            analyzer=proto.analyzer, sub_field_schemas=sub_field_schemas,
        )

        return field_schema

    def _parse_sort_order(self, proto):
        if proto is None:
            return None

        return SortOrder.ASC if proto == SortOrder.ASC else SortOrder.DESC

    def _parse_sort_mode(self, proto):
        if proto is None:
            return None

        if proto == SortMode.AVG:
            return SortMode.AVG
        elif proto == SortMode.MIN:
            return SortMode.MIN
        else:
            return SortMode.MAX

    def _parse_geo_distance_type(self, proto):
        if proto is None:
            return None

        return GeoDistanceType.ARC if proto == GeoDistanceType.ARC else GeoDistanceType.PLANE

    def _parse_index_setting(self, proto):
        index_setting = IndexSetting()
        index_setting.routing_fields.extend(proto.routing_fields)
        return index_setting

    def _parse_nested_filter(self, proto):
        return None

    def _parse_index_sorter(self, proto):
        sorter = None
        if proto.HasField('field_sort'):
            sorter = FieldSort(
                proto.field_sort.field_name, sort_order=self._parse_sort_order(proto.field_sort.order),
                sort_mode=self._parse_sort_mode(proto.field_sort.mode),
                nested_filter=self._parse_nested_filter(proto.field_sort.nested_filter))
        elif proto.HasField('geo_distance_sort'):
            sorter = GeoDistanceSort(
                proto.geo_distance_sort.field_name,
                proto.geo_distance_sort.points,
                sort_order=self._parse_sort_order(proto.geo_distance_sort.order),
                sort_mode=self._parse_sort_mode(proto.geo_distance_sort.mode),
                geo_distance_type=self._parse_geo_distance_type(proto.geo_distance_sort.distance_type),
                nested_filter=self._parse_nested_filter(proto.geo_distance_sort.nested_filter)
            )
        elif proto.HasField('score_sort'):
            sorter = ScoreSort(
                sort_order=self._parse_sort_order(proto.score_sort.order)
            )
        elif proto.HasField('pk_sort'):
            sorter = PrimaryKeySort(
                sort_order=self._parse_sort_order(proto.pk_sort.order)
            )
        else:
            raise OTSClientError(
                "Can't find any index sorter."
            )

        return sorter

    def _parse_index_sort(self, proto):
        sorters = []
        for sorter_proto in proto.sorter:
            sorters.append(self._parse_index_sorter(sorter_proto))
        index_sort = Sort(sorters)
        return index_sort


    def _parse_index_meta(self, proto):
        fields = []
        for field_schema_proto in proto.field_schemas:
            fields.append(self._parse_field_schema(field_schema_proto))

        index_setting = self._parse_index_setting(proto.index_setting)
        index_sort = self._parse_index_sort(proto.index_sort)

        index_meta = SearchIndexMeta(fields, index_setting, index_sort)
        return index_meta

    def _parse_sync_stat(self, proto):
        sync_stat = SyncStat(SyncPhase.FULL if proto.sync_phase == search_pb.FULL else SyncPhase.INCR, proto.current_sync_timestamp)
        return sync_stat

    def _decode_list_search_index(self, body):
        proto = search_pb.ListSearchIndexResponse()
        proto.ParseFromString(body)

        indices = []
        for index in proto.indices:
            indices.append(self._parse_index_info(index))
        return indices, proto

    def _decode_describe_search_index(self, body):
        proto = search_pb.DescribeSearchIndexResponse()
        proto.ParseFromString(body)

        index_meta = self._parse_index_meta(proto.schema)
        sync_stat = self._parse_sync_stat(proto.sync_stat)

        return (index_meta, sync_stat), proto

    def _decode_create_search_index(self, body):
        proto = search_pb.CreateSearchIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_delete_search_index(self, body):
        proto = search_pb.DeleteSearchIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_search(self, body):
        proto = search_pb.SearchResponse()
        proto.ParseFromString(body)
        rows = []
        for row_proto in proto.rows:
            input_stream = PlainBufferInputStream(row_proto)
            codedInputStream = PlainBufferCodedInputStream(input_stream)
            primary_key_columns, attribute_columns = codedInputStream.read_row()
            rows.append((primary_key_columns, attribute_columns))
        total_count = proto.total_hits
        is_all_succeed = proto.is_all_succeed

        agg_results = []
        if proto.aggs is not None and len(proto.aggs) > 0:
            proto_result = search_pb.AggregationsResult()
            proto_result.ParseFromString(proto.aggs)
            self._decode_agg_results(proto_result, agg_results)

        group_by_results = []
        if proto.group_bys is not None and len(proto.group_bys) > 0:
            proto_result = search_pb.GroupBysResult()
            proto_result.ParseFromString(proto.group_bys)
            self._decode_group_by_results(proto_result, group_by_results)

        search_response = SearchResponse(rows, agg_results, group_by_results, 
                                         proto.next_token, is_all_succeed, total_count)
        return (search_response), proto

    def _decode_agg_results(self, proto_result, agg_results):
        if proto_result is not None:
            for agg_result in proto_result.agg_results:
                name = agg_result.name
                value = self._decode_agg(agg_result.type, agg_result.agg_result)
                agg_results.append(AggResult(name, value))

    def _decode_group_by_results(self, proto_result, group_by_results):
        if proto_result is not None:
            for group_result in proto_result.group_by_results:
                name = group_result.name
                items = self._decode_group_by(group_result.type, group_result.group_by_result)
                group_by_results.append(GroupByResult(name, items))

    def _decode_agg(self, type, body):
        if search_pb.AGG_AVG == type:
            proto = search_pb.AvgAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_MAX == type:
            proto = search_pb.MaxAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_MIN == type:
            proto = search_pb.MinAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_SUM == type:
            proto = search_pb.SumAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_COUNT == type:
            proto = search_pb.CountAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_DISTINCT_COUNT == type:
            proto = search_pb.DistinctCountAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_TOP_ROWS == type:
            proto = search_pb.TopRowsAggregationResult()
            proto.ParseFromString(body)
            rows = []
            for row_proto in proto.rows:
                input_stream = PlainBufferInputStream(row_proto)
                codedInputStream = PlainBufferCodedInputStream(input_stream)
                primary_key_columns, attribute_columns = codedInputStream.read_row()
                rows.append((primary_key_columns, attribute_columns))
            return rows
        else:
            raise OTSClientError('unsupport type:%s' % str(type))

    def _decode_group_by(self, type, body):
        if search_pb.GROUP_BY_FIELD == type:
            proto = search_pb.GroupByFieldResult()
            proto.ParseFromString(body)

            result_items = []
            for item in proto.group_by_field_result_items:
                sub_group_by_results = []
                sub_agg_results = []
                self._decode_group_by_results(item.sub_group_bys_result, sub_group_by_results)
                self._decode_agg_results(item.sub_aggs_result, sub_agg_results)
                
                result_item = GroupByFieldResultItem(item.key, item.row_count,
                                                     sub_agg_results, sub_group_by_results)
                result_items.append(result_item)
            return result_items

        elif search_pb.GROUP_BY_RANGE == type:
            proto = search_pb.GroupByRangeResult()
            proto.ParseFromString(body)

            result_items = []
            for item in proto.group_by_range_result_items:
                sub_group_by_results = []
                sub_agg_results = []
                self._decode_group_by_results(item.sub_group_bys_result, sub_group_by_results)
                self._decode_agg_results(item.sub_aggs_result, sub_agg_results)
                
                result_item = GroupByRangeResultItem(item.range_from, item.range_to, item.row_count,
                                                     sub_agg_results, sub_group_by_results)
                result_items.append(result_item)
            return result_items
        elif search_pb.GROUP_BY_FILTER == type:
            proto = search_pb.GroupByFilterResult()
            proto.ParseFromString(body)

            result_items = []
            for item in proto.group_by_filter_result_items:
                sub_group_by_results = []
                sub_agg_results = []
                self._decode_group_by_results(item.sub_group_bys_result, sub_group_by_results)
                self._decode_agg_results(item.sub_aggs_result, sub_agg_results)
                
                result_item = GroupByFilterResultItem(item.row_count,
                                                      sub_agg_results, sub_group_by_results)
                result_items.append(result_item)
            return result_items
        elif search_pb.GROUP_BY_GEO_DISTANCE == type:
            proto = search_pb.GroupByGeoDistanceResult()
            proto.ParseFromString(body)

            result_items = []
            for item in proto.group_by_geo_distance_result_items:
                sub_group_by_results = []
                sub_agg_results = []
                self._decode_group_by_results(item.sub_group_bys_result, sub_group_by_results)
                self._decode_agg_results(item.sub_aggs_result, sub_agg_results)
                
                result_item = GroupByGeoDistanceResultItem(item.range_from, item.range_to, item.row_count,
                                                           sub_agg_results, sub_group_by_results)
                result_items.append(result_item)
            return result_items
        else:
            raise OTSClientError('unsupport type:%s' % str(type))

    def _decode_compute_splits(self, body):
        proto = search_pb.ComputeSplitsResponse()
        proto.ParseFromString(body)

        session_id = str(proto.session_id)
        splits_size = proto.splits_size

        compute_splits_response = ComputeSplitsResponse(session_id, splits_size)
        return compute_splits_response, proto

    def _decode_parallel_scan(self, body):
        proto = search_pb.ParallelScanResponse()
        proto.ParseFromString(body)
        rows = []
        for row_proto in proto.rows:
            input_stream = PlainBufferInputStream(row_proto)
            codedInputStream = PlainBufferCodedInputStream(input_stream)
            primary_key_columns, attribute_columns = codedInputStream.read_row()
            rows.append((primary_key_columns, attribute_columns))

        response = ParallelScanResponse(rows, proto.next_token)
        return response, proto

    def _decode_create_index(self, body):
        proto = pb.CreateIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_delete_index(self, body):
        proto = pb.DropIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_start_local_transaction(self, body):
        proto = pb.StartLocalTransactionResponse()
        proto.ParseFromString(body)

        transaction_id = proto.transaction_id

        return transaction_id, proto

    def _decode_commit_transaction(self, body):
        proto = pb.CommitTransactionResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_abort_transaction(self, body):
        proto = pb.AbortTransactionResponse()
        proto.ParseFromString(body)

        return None, proto
