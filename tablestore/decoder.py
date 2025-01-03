# -*- coding: utf8 -*-

import google.protobuf.text_format as text_format

from tablestore.metadata import *
from tablestore.aggregation import *
from tablestore.group_by import *
from tablestore.plainbuffer.plain_buffer_builder import *

import tablestore.protobuf.table_store_pb2 as pb
import tablestore.protobuf.table_store_filter_pb2 as filter_pb
import tablestore.protobuf.search_pb2 as search_pb
import tablestore.protobuf.timeseries_pb2 as timeseries_pb2

from dataprotocol.SQLResponseColumns import *
from tablestore.flatbuffer.flat_buffer_decoder import *


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
            'UpdateSearchIndex'     : self._decode_update_search_index,
            'Search'                : self._decode_search,
            'ComputeSplits'         : self._decode_compute_splits,
            'ParallelScan'          : self._decode_parallel_scan,
            'CreateIndex'           : self._decode_create_index,
            'DropIndex'             : self._decode_delete_index,
            'StartLocalTransaction' : self._decode_start_local_transaction,
            'CommitTransaction'     : self._decode_commit_transaction,
            'AbortTransaction'      : self._decode_abort_transaction,
            'SQLQuery'              : self._decode_exe_sql_query,
            'PutTimeseriesData'     : self._decode_put_timeseries_data,
            'GetTimeseriesData'     : self._decode_get_timeseries_data,
            'CreateTimeseriesTable' : self._decode_create_timeseries_table,
            'ListTimeseriesTable'   : self._decode_list_timeseries_table,
            'DeleteTimeseriesTable' : self._decode_delete_timeseries_table,
            'DescribeTimeseriesTable': self._decode_describe_timeseries_table,
            'UpdateTimeseriesTable' : self._decode_update_timeseries_table,
            'UpdateTimeseriesMeta'  : self._decode_update_timeseries_meta,
            'DeleteTimeseriesMeta'  : self._decode_delete_timeseries_meta,
            'QueryTimeseriesMeta'   : self._decode_query_timeseries_meta,
        }

    def _parse_string(self, string):
        if string == '':
            return None
        else:
            return string

    def _parse_column_type(self, column_type_enum):
        reverse_enum_map = {
            pb.INTEGER: 'INTEGER',
            pb.STRING: 'STRING',
            pb.BINARY: 'BINARY'
        }
        if column_type_enum in reverse_enum_map:
            return reverse_enum_map[column_type_enum]
        else:
            raise OTSClientError("invalid value for column type: %s" % str(column_type_enum))

    def _parse_column_option(self, column_option_enum):
        reverse_enum_map = {
            pb.AUTO_INCREMENT: PK_AUTO_INCR,
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
        max_deviation_time = proto.deviation_cell_version_in_sec

        allow_update = True
        if proto.HasField('allow_update'):
            allow_update = proto.allow_update
        return TableOptions(time_to_live, max_versions, max_deviation_time, allow_update)

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
        rows = {}
        for table_item in proto:
            rows[table_item.table_name] = self._parse_get_row_item(table_item.rows, table_item.table_name)
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

    def _decode_create_table(self, body, request_id):
        proto = pb.CreateTableResponse()
        proto.ParseFromString(body)
        return None, proto

    def _decode_list_table(self, body, request_id):
        proto = pb.ListTableResponse()
        proto.ParseFromString(body)
        names = tuple(proto.table_names)
        return names, proto

    def _decode_delete_table(self, body, request_id):
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

    def _decode_describe_table(self, body, request_id):
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
        describe_table_response = DescribeTableResponse(table_meta, table_options, reserved_throughput_details,
                                                        secondary_indexes)
        describe_table_response.set_request_id(request_id)
        return describe_table_response, proto

    def _decode_update_table(self, body, request_id):
        proto = pb.UpdateTableResponse()
        proto.ParseFromString(body)

        reserved_throughput_details = self._parse_reserved_throughput_details(proto.reserved_throughput_details)
        table_options = self._parse_table_options(proto.table_options)
        update_table_response = UpdateTableResponse(reserved_throughput_details, table_options)
        update_table_response.set_request_id(request_id)

        return update_table_response, proto

    def _decode_get_row(self, body, request_id):
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

    def _decode_put_row(self, body, request_id):
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

    def _decode_update_row(self, body, request_id):
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

    def _decode_delete_row(self, body, request_id):
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

    def _decode_batch_get_row(self, body, request_id):
        proto = pb.BatchGetRowResponse()
        proto.ParseFromString(body)

        rows = self._parse_batch_get_row(proto.tables)
        return rows, proto

    def _decode_batch_write_row(self, body, request_id):
        proto = pb.BatchWriteRowResponse()
        proto.ParseFromString(body)

        rows = self._parse_batch_write_row(proto.tables)
        return rows, proto

    def _decode_get_range(self, body, request_id):
        proto = pb.GetRangeResponse()
        proto.ParseFromString(body)

        capacity_unit = self._parse_capacity_unit(proto.consumed.capacity_unit)

        next_start_pk = None
        row_list = []
        if len(proto.next_start_primary_key) != 0:
            inputStream = PlainBufferInputStream(proto.next_start_primary_key)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            next_start_pk, att = codedInputStream.read_row()

        if len(proto.rows) != 0:
            inputStream = PlainBufferInputStream(proto.rows)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            row_list = codedInputStream.read_rows()

        next_token = proto.next_token

        return (capacity_unit, next_start_pk, row_list, next_token), proto

    def decode_response(self, api_name, response_body, request_id):
        if api_name not in self.api_decode_map:
            raise OTSClientError("No PB decode method for API %s" % api_name)

        handler = self.api_decode_map[api_name]
        return handler(response_body, request_id)

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
        elif proto == search_pb.DATE:
            field_type = FieldType.DATE

        return field_type

    def _parse_field_schema(self, proto):
        sub_field_schemas = []
        for sub_field_schema_proto in proto.field_schemas:
            sub_field_schemas.append(self._parse_field_schema(sub_field_schema_proto))

        date_formats = []
        for df in proto.date_formats:
            date_formats.append(df)

        is_virtual_field = False
        source_fields = []

        if proto.HasField('is_virtual_field'):
            is_virtual_field = proto.is_virtual_field

            for source_field in proto.source_field_names:
                source_fields.append(source_field)

        analyzer_parameter = None
        if proto.HasField('analyzer_parameter') and len(proto.analyzer_parameter) > 0:
            analyzer_parameter = self._parse_analyzer_parameter(proto.analyzer, proto.analyzer_parameter)

        vector_options = None
        if proto.HasField("vector_options"):
            vector_options = self._parse_vector_options(proto.vector_options)

        field_schema = FieldSchema(
            proto.field_name, self._parse_field_type(proto.field_type),
            index=proto.index, store=proto.store, is_array=proto.is_array,
            enable_sort_and_agg=proto.enable_sort_and_agg,
            analyzer=proto.analyzer, sub_field_schemas=sub_field_schemas,
            analyzer_parameter=analyzer_parameter, date_formats=date_formats,
            is_virtual_field=is_virtual_field, source_fields=source_fields, vector_options=vector_options
        )

        return field_schema

    def _parse_vector_options(self, proto_vector_options):
        if proto_vector_options is None:
            return None

        return VectorOptions(data_type=proto_vector_options.data_type, metric_type=proto_vector_options.metric_type,
                             dimension=proto_vector_options.dimension)

    def _parse_analyzer_parameter(self, analyzer, analyzer_parameter):
        try:
            if analyzer == AnalyzerType.SINGLEWORD:
                parameter = search_pb.SingleWordAnalyzerParameter()
                parameter.ParseFromString(analyzer_parameter)
                return SingleWordAnalyzerParameter(parameter.case_sensitive, parameter.delimit_word)
            elif analyzer == AnalyzerType.SPLIT:
                parameter = search_pb.SplitAnalyzerParameter()
                parameter.ParseFromString(analyzer_parameter)
                return SplitAnalyzerParameter(parameter.delimiter)
            elif analyzer == AnalyzerType.FUZZY:
                parameter = search_pb.FuzzyAnalyzerParameter()
                parameter.ParseFromString(analyzer_parameter)
                return FuzzyAnalyzerParameter(parameter.min_chars, parameter.max_chars)
            else:
                return None
        except Exception as e:
            error_message = 'parse analyzer_parameter failed, please contact tablestore, exception:%s.' % (str(e))
            self.logger.error(error_message)
            raise e

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
        sync_stat = SyncStat(SyncPhase.FULL if proto.sync_phase == search_pb.FULL else SyncPhase.INCR,
                             proto.current_sync_timestamp)
        return sync_stat

    def _decode_list_search_index(self, body, request_id):
        proto = search_pb.ListSearchIndexResponse()
        proto.ParseFromString(body)

        indices = []
        for index in proto.indices:
            indices.append(self._parse_index_info(index))
        return indices, proto

    def _decode_describe_search_index(self, body, request_id):
        proto = search_pb.DescribeSearchIndexResponse()
        proto.ParseFromString(body)

        index_meta = self._parse_index_meta(proto.schema)
        sync_stat = self._parse_sync_stat(proto.sync_stat)
        index_meta.time_to_live = proto.time_to_live

        return (index_meta, sync_stat), proto

    def _decode_create_search_index(self, body, request_id):
        proto = search_pb.CreateSearchIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_update_search_index(self, body, request_id):
        proto = search_pb.UpdateSearchIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_delete_search_index(self, body, request_id):
        proto = search_pb.DeleteSearchIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_search(self, body, request_id):
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

        search_hits = []
        if proto.search_hits is not None and len(proto.search_hits) > 0:
            if len(proto.search_hits) == len(rows):
                self._decode_search_hits(proto.search_hits, rows, search_hits, request_id)
            else:
                raise OTSServiceError(
                    "Tablestore search response occur error, row count[%d] != search_hit count[%d], request_id:%s"
                    % (len(rows), len(proto.search_hits), request_id))

        search_response = SearchResponse(rows, agg_results, group_by_results,
                                         proto.next_token, is_all_succeed, total_count, search_hits)
        search_response.set_request_id(request_id)
        return (search_response), proto

    def _decode_search_hits(self, proto_results, rows, search_hits, request_id):
        row_index = 0
        for hit in proto_results:
            score = hit.score
            nested_doc_offset = hit.nested_doc_offset

            highlight_result = None
            if hit.highlight_result is not None:
                highlight_fields = []
                for field in hit.highlight_result.highlight_fields:
                    highlight_field = HighlightField(field.field_name, field.field_fragments)
                    highlight_fields.append(highlight_field)
                highlight_result = HighlightResult(highlight_fields)

            search_inner_hits = []
            for innerHit in hit.search_inner_hits:
                path = innerHit.path
                search_hits2 = []
                self._decode_search_hits(innerHit.search_hits, [], search_hits2, request_id)
                search_inner_hits.append(SearchInnerHit(path, search_hits2))

            row = rows[row_index] if len(rows) != 0 else None
            row_index += 1

            search_hit = SearchHit(row, score, highlight_result, search_inner_hits, nested_doc_offset)
            search_hits.append(search_hit)

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

    def _decode_agg(self, agg_type, body):
        if search_pb.AGG_AVG == agg_type:
            proto = search_pb.AvgAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_MAX == agg_type:
            proto = search_pb.MaxAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_MIN == agg_type:
            proto = search_pb.MinAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_SUM == agg_type:
            proto = search_pb.SumAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_COUNT == agg_type:
            proto = search_pb.CountAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_DISTINCT_COUNT == agg_type:
            proto = search_pb.DistinctCountAggregationResult()
            proto.ParseFromString(body)
            return proto.value
        elif search_pb.AGG_TOP_ROWS == agg_type:
            proto = search_pb.TopRowsAggregationResult()
            proto.ParseFromString(body)
            rows = []
            for row_proto in proto.rows:
                input_stream = PlainBufferInputStream(row_proto)
                codedInputStream = PlainBufferCodedInputStream(input_stream)
                primary_key_columns, attribute_columns = codedInputStream.read_row()
                rows.append((primary_key_columns, attribute_columns))
            return rows
        elif search_pb.AGG_PERCENTILES == agg_type:
            proto = search_pb.PercentilesAggregationResult()
            proto.ParseFromString(body)
            percentiles = []
            for percentile in proto.percentiles_aggregation_items:
                percentiles.append(PercentilesResultItem(percentile.key, self._decode_column_value(percentile.value)))
            return percentiles
        else:
            raise OTSClientError('unsupport aggregation type:%s' % str(agg_type))

    def _decode_group_by(self, groupby_type, body):
        if search_pb.GROUP_BY_FIELD == groupby_type:
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

        elif search_pb.GROUP_BY_RANGE == groupby_type:
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
        elif search_pb.GROUP_BY_FILTER == groupby_type:
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
        elif search_pb.GROUP_BY_GEO_DISTANCE == groupby_type:
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
        elif search_pb.GROUP_BY_HISTOGRAM == groupby_type:
            proto = search_pb.GroupByHistogramResult()
            proto.ParseFromString(body)

            result_items = []
            for item in proto.group_by_histogra_items:
                sub_group_by_results = []
                sub_agg_results = []
                self._decode_group_by_results(item.sub_group_bys_result, sub_group_by_results)
                self._decode_agg_results(item.sub_aggs_result, sub_agg_results)

                result_item = GroupByHistogramResultItem(self._decode_column_value(item.key), item.value,
                                                         sub_agg_results, sub_group_by_results)
                result_items.append(result_item)
            return result_items
        else:
            raise OTSClientError('unsupport group by type:%s' % str(groupby_type))

    def _decode_column_value(self, value):
        if len(value) == 0:
            raise OTSClientError('value length is 0')

        cell_type = value[0]
        if python_version == 2:
            cell_type = ord(cell_type)

        if cell_type == VT_INTEGER:
            return struct.unpack('<q', value[1:9])[0]
        elif cell_type == VT_DOUBLE:
            return struct.unpack('d', value[1:9])[0]
        elif cell_type == VT_BOOLEAN:
            return struct.unpack('<?', value[1:2])[0]
        else:
            return value[1:]

    def _decode_compute_splits(self, body, request_id):
        proto = search_pb.ComputeSplitsResponse()
        proto.ParseFromString(body)

        session_id = proto.session_id.decode('utf-8')

        splits_size = proto.splits_size

        compute_splits_response = ComputeSplitsResponse(session_id, splits_size)
        compute_splits_response.set_request_id(request_id)
        return compute_splits_response, proto

    def _decode_parallel_scan(self, body, request_id):
        proto = search_pb.ParallelScanResponse()
        proto.ParseFromString(body)
        rows = []
        for row_proto in proto.rows:
            input_stream = PlainBufferInputStream(row_proto)
            codedInputStream = PlainBufferCodedInputStream(input_stream)
            primary_key_columns, attribute_columns = codedInputStream.read_row()
            rows.append((primary_key_columns, attribute_columns))

        response = ParallelScanResponse(rows, proto.next_token)
        response.set_request_id(request_id)
        return response, proto

    def _decode_create_index(self, body, request_id):
        proto = pb.CreateIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_delete_index(self, body, request_id):
        proto = pb.DropIndexResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_start_local_transaction(self, body, request_id):
        proto = pb.StartLocalTransactionResponse()
        proto.ParseFromString(body)

        transaction_id = proto.transaction_id

        return transaction_id, proto

    def _decode_commit_transaction(self, body, request_id):
        proto = pb.CommitTransactionResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_abort_transaction(self, body, request_id):
        proto = pb.AbortTransactionResponse()
        proto.ParseFromString(body)

        return None, proto

    def _decode_put_timeseries_data(self, body, request_id):
        proto = timeseries_pb2.PutTimeseriesDataResponse()
        proto.ParseFromString(body)

        resp = PutTimeseriesDataResponse()
        for failed in proto.failed_rows:
            resp.failedRows.append(self._parse_timeseries_failed_rows(failed))
        return resp, proto

    def _decode_exe_sql_query(self, body, request_id):
        proto = pb.SQLQueryResponse()
        proto.ParseFromString(body)

        table_capacity_units = []
        table_consumes_list = []
        if len(proto.consumes) != 0:
            table_consumes_list.extend(proto.consumes)
        for table_consume in table_consumes_list:
            capacity_unit = self._parse_capacity_unit(table_consume.consumed.capacity_unit)
            table_capacity_unit = (table_consume.table_name, capacity_unit)
            table_capacity_units.append(table_capacity_unit)

        search_capacity_units = []
        search_consumes_list = []
        if len(proto.search_consumes) != 0:
            search_consumes_list.extend(proto.search_consumes)
        for search_consume in search_consumes_list:
            capacity_unit = self._parse_capacity_unit(search_consume.consumed.capacity_unit)
            search_capacity_unit = (search_consume.table_name, capacity_unit)
            search_capacity_units.append(search_capacity_unit)

        rows = []
        if len(proto.rows) != 0:
            columns = flat_buffer_decoder.format_flat_buffer_columns(
                SQLResponseColumns.GetRootAsSQLResponseColumns(proto.rows))
            rows = flat_buffer_decoder.columns_to_rows(columns)
        return (rows, table_capacity_units, search_capacity_units), proto

    def _decode_create_timeseries_table(self, body, request_id):
        proto = timeseries_pb2.CreateTimeseriesTableResponse()
        proto.ParseFromString(body)
        return None, proto

    def _decode_list_timeseries_table(self, body, request_id):
        proto = timeseries_pb2.ListTimeseriesTableResponse()
        proto.ParseFromString(body)
        tables = []
        for metaproto in proto.table_metas:
            tables.append(self._parse_timeseries_table_meta(metaproto))
        return tables, proto

    def _parse_timeseries_table_meta(self, table_meta_proto):
        meta = TimeseriesTableMeta(table_meta_proto.table_name)
        meta.status = table_meta_proto.status
        meta.timeseries_table_options = self._parse_timeseries_table_options(table_meta_proto.table_options)
        meta.timeseries_meta_options = self._parse_timeseries_meta_options(table_meta_proto.meta_options)
        timeseries_keys = []
        for key in table_meta_proto.timeseries_key_schema:
            timeseries_keys.append(key)
        meta.timeseries_keys = timeseries_keys
        meta.field_primary_keys = self._parse_field_primary_key_schema(table_meta_proto.field_primary_key_schema)
        return meta

    def _parse_timeseries_table_options(self, table_option_proto):
        return TimeseriesTableOptions(table_option_proto.time_to_live)

    def _parse_timeseries_meta_options(self, table_meta_proto):
        if table_meta_proto is None:
            meta_option = None
        else:
            allow_update_attributes = table_meta_proto.allow_update_attributes if table_meta_proto.HasField('allow_update_attributes') else None
            meta_time_to_live = table_meta_proto.meta_time_to_live if table_meta_proto.HasField('meta_time_to_live') else None
            meta_option = TimeseriesMetaOptions(meta_time_to_live, allow_update_attributes)
        return meta_option

    def _parse_field_primary_key_schema(self, field_primary_key_schema_protos):
        if field_primary_key_schema_protos is None or len(field_primary_key_schema_protos) == 0:
            schema_list = None
        else:
            schema_list = []
            for proto in field_primary_key_schema_protos:
                schema_list.append(self._parse_primary_key_schema(proto))
        return schema_list

    def _parse_primary_key_schema(self, proto):
        if proto.HasField('option'):
            return proto.name, self._parse_column_type(proto.type), self._parse_column_option(proto.option)
        else:
            return proto.name, self._parse_column_type(proto.type)

    def _decode_delete_timeseries_table(self, body, request_id):
        proto = timeseries_pb2.DeleteTimeseriesTableResponse()
        proto.ParseFromString(body)
        return None, proto

    def _decode_describe_timeseries_table(self, body, request_id):
        proto = timeseries_pb2.DescribeTimeseriesTableResponse()
        proto.ParseFromString(body)
        resp = DescribeTimeseriesTableResponse(self._parse_timeseries_table_meta(proto.table_meta))
        return resp, proto

    def _decode_update_timeseries_table(self, body, request_id):
        proto = timeseries_pb2.UpdateTimeseriesTableResponse()
        proto.ParseFromString(body)
        return None, proto

    def _decode_update_timeseries_meta(self, body, request_id):
        proto = timeseries_pb2.UpdateTimeseriesMetaResponse()
        proto.ParseFromString(body)
        fails = []
        for failed in proto.failed_rows:
            fails.append(self._parse_timeseries_failed_rows(failed))

        return UpdateTimeseriesMetaResponse(fails), proto

    def _parse_timeseries_failed_rows(self, proto):
        result = FailedRowResult()
        result.index = proto.row_index
        result.error.code = proto.error_code
        result.error.message = proto.error_message
        return result

    def _decode_delete_timeseries_meta(self, body, request_id):
        proto = timeseries_pb2.DeleteTimeseriesMetaResponse()
        proto.ParseFromString(body)
        fails = []
        for failed in proto.failed_rows:
            fails.append(self._parse_timeseries_failed_rows(failed))

        return DeleteTimeseriesMetaResponse(fails), proto

    def _decode_query_timeseries_meta(self, body, request_id):
        proto = timeseries_pb2.QueryTimeseriesMetaResponse()
        proto.ParseFromString(body)
        response = QueryTimeseriesMetaResponse()
        response.totalHits = proto.total_hit
        if proto.HasField('next_token'):
            response.nextToken = proto.next_token
        response.timeseriesMetas = self._parse_timeseries_meta(proto.timeseries_metas)
        return response, proto

    def _parse_timeseries_meta(self, protos):
        res = []
        for proto in protos:
            attr = None
            if len(proto.attributes) > 0:
                attr = self._parse_timeseries_tag_or_attribute(proto.attributes)
            result = TimeseriesMeta(self._parse_timeseries_key(proto.time_series_key), attr, proto.update_time)
            res.append(result)
        return res

    def _parse_timeseries_key(self, proto):
        key = TimeseriesKey()
        key.data_source = proto.source
        key.measurement_name = proto.measurement
        if len(proto.tag_list) > 0:
            key.tags = self._parse_tag_list(proto.tag_list)
        return key

    def _parse_tag_list(self, tags):
        res = {}
        for tag in tags:
            res[tag.name] = tag.value
        return res

    def _decode_get_timeseries_data(self, body, request_id):
        proto = timeseries_pb2.GetTimeseriesDataResponse()
        proto.ParseFromString(body)
        res = GetTimeseriesDataResponse()
        if proto.HasField('next_token'):
            res.nextToken = proto.next_token
        if len(proto.rows_data) != 0:
            inputStream = PlainBufferInputStream(proto.rows_data)
            codedInputStream = PlainBufferCodedInputStream(inputStream)
            row_list = codedInputStream.read_rows()
            rows = []
            for row in row_list:
                rows.append(self._parse_timeseries_rows_plain_buffer(row))
            res.rows = rows
        return res, proto

    def _parse_timeseries_rows_plain_buffer(self, row):
        measurement = None
        source = None
        tags_str = None
        time = -1
        count = 0
        tags = {}
        for index in range(0, len(row.primary_key)):
            pkcell = row.primary_key[index]
            if pkcell[0] == "_m_name":
                measurement = pkcell[1]
            elif pkcell[0] == "_data_source":
                source = pkcell[1]
            elif pkcell[0] == "_tags":
                tags_str = pkcell[1]
            elif pkcell[0] == "_time":
                count = index
                time = pkcell[1]
                break
            elif pkcell[0] != "_#h":
                tags[pkcell[0]] = pkcell[1]
        if tags_str is not None:
            tags.update(self._parse_timeseries_tag_or_attribute(tags_str))
        if time == -1:
            raise OTSClientError('no time column in timeesries row')

        key = TimeseriesKey(measurement, source, tags)
        fields = self._parse_attribute_columns(row.attribute_columns)
        for item in row.primary_key[count + 1:]:
            fields[item[0]] = item[1]

        return TimeseriesRow(key, fields, time)

    def _parse_attribute_columns(self, columns):
        res = {}
        for i in range(0, len(columns)):
            key = columns[i][0].split(":")[0]
            value = columns[i][1]
            res[key] = value
        return res

    def _parse_timeseries_tag_or_attribute(self, tag: str):
        res = {}
        if not tag.startswith("[") or not tag.endswith("]") or len(tag) < 2:
            raise OTSClientError('invalid tag or attribute value:%s' % str(tag))
        if len(tag) == 2:
            return res
        strs = tag[1:len(tag) - 1]
        for items in strs.split(","):
            if not items.startswith("\"") or not items.endswith("\""):
                raise OTSClientError('invalid tag or attribute value:%s' % str(tag))
            x = items[1:len(items) - 1].split("=")
            if len(x) != 2:
                raise OTSClientError('invalid tag or attribute value:%s' % str(tag))
            if x[0] == "" or x[1] == "":
                raise OTSClientError('invalid tag or attribute value:%s' % str(tag))
            res[x[0]] = x[1]
        return res
