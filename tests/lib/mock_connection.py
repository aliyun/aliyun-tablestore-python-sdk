# -*- coding: utf8 -*-

import time
import hmac
import hashlib
import base64

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import google.protobuf.text_format as text_format

from tablestore import *
from tablestore.metadata import *
from tablestore.error import *
import tablestore.protobuf.table_store_pb2 as pb2


class MockConnection(object):

    def __init__(self, host, path, timeout=0, maxsize=50):
        self.host = host
        self.path = path
        self.user_key = 'accesskey'

        self.api_mock_map = {
            'CreateTable'       : self._mock_create_table,
            'ListTable'         : self._mock_list_table,
            'DeleteTable'       : self._mock_delete_table,
            'DescribeTable'     : self._mock_describe_table,
            'UpdateTable'       : self._mock_update_table,
            'GetRow'            : self._mock_get_row,
            'PutRow'            : self._mock_put_row,
            'UpdateRow'         : self._mock_update_row,
            'DeleteRow'         : self._mock_delete_row,
            'BatchGetRow'       : self._mock_batch_get_row,
            'BatchWriteRow'     : self._mock_batch_write_row,
            'GetRange'          : self._mock_get_range
        }

    def send_receive(self, query, headers, body):
        api_name = query[1:]
        handler = self.api_mock_map.get(api_name)
        if handler is None:
            raise OTSClientError("API %s is not supported." % api_name)

        proto_body = handler(body)

        response_body = proto_body.SerializeToString()

        resp_headers = {
            'x-ots-date' : time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'x-ots-requestid' : '123456789-123456789',
            'x-ots-contentmd5' : base64.b64encode(hashlib.md5(response_body).digest()),
            'x-ots-contenttype' : 'OTS protobuf'
        }
        signature = self._make_response_signature(query, resp_headers)
        resp_headers['authorization'] = 'OTS accessid:%s' % signature

        return 200, 'OK', resp_headers, response_body

    def _make_headers_string(self, headers):
        headers_item = ["%s:%s" % (k.lower(), v.strip()) for k, v in headers.iteritems() \
                        if k.startswith('x-ots-') and k != 'x-ots-signature']
        return "\n".join(sorted(headers_item))

    def _call_signature_method(self, signature_string):
        signature = base64.b64encode(hmac.new(
            self.user_key, signature_string, hashlib.sha1
        ).digest())
        return signature

    def _make_response_signature(self, query, headers):
        uri = urlparse.urlparse(query)[2]
        headers_string = self._make_headers_string(headers)

        signature_string = headers_string + '\n' + uri
        signature = self._call_signature_method(signature_string)
        return signature

    def _mock_create_table(self, body):
        proto = pb2.CreateTableResponse()
        return proto

    def _mock_list_table(self, body):
        proto = pb2.ListTableResponse()
        proto.table_names.append('test_table1')
        proto.table_names.append('test_table2')
        return proto

    def _mock_delete_table(self, body):
        proto = pb2.DeleteTableResponse()
        return proto

    def _mock_describe_table(self, body):
        proto = pb2.DescribeTableResponse()

        proto.table_meta.table_name = 'test_table'
        primary_key = proto.table_meta.primary_key.add()
        primary_key.name = 'PK1'
        primary_key.type = pb2.STRING
        primary_key = proto.table_meta.primary_key.add()
        primary_key.name = 'PK2'
        primary_key.type = pb2.INTEGER

        proto.reserved_throughput_details.capacity_unit.read = 1000
        proto.reserved_throughput_details.capacity_unit.write = 100
        proto.reserved_throughput_details.last_increase_time = int(123456)
        proto.reserved_throughput_details.last_decrease_time = int(123456)
        proto.reserved_throughput_details.number_of_decreases_today = 5

        return proto

    def _mock_update_table(self, body):
        proto = pb2.UpdateTableResponse()
        proto.reserved_throughput_details.capacity_unit.read = 10
        proto.reserved_throughput_details.capacity_unit.write = 10
        proto.reserved_throughput_details.last_increase_time = int(123456)
        #proto.reserved_throughput_details.last_decrease_time = int(123456)
        proto.reserved_throughput_details.number_of_decreases_today = 5
        return proto

    def _mock_get_row(self, body):
        proto = pb2.GetRowResponse()
        proto.consumed.capacity_unit.read = 10
        primary_key_columns = proto.row.primary_key_columns.add()
        primary_key_columns.name = 'PK1'
        primary_key_columns.value.type = pb2.STRING
        primary_key_columns.value.v_string = 'Hello'
        primary_key_columns = proto.row.primary_key_columns.add()
        primary_key_columns.name = 'PK2'
        primary_key_columns.value.type = pb2.BINARY
        primary_key_columns.value.v_binary = 'World'

        column = proto.row.attribute_columns.add()
        column.name = 'COL1'
        column.value.type = pb2.STRING
        column.value.v_string = 'test'
        column = proto.row.attribute_columns.add()
        column.name = 'COL2'
        column.value.type = pb2.INTEGER
        column.value.v_int = 100

        return proto

    def _mock_put_row(self, body):
        proto = pb2.PutRowResponse()
        proto.consumed.capacity_unit.write = 10
        return proto

    def _mock_update_row(self, body):
        proto = pb2.UpdateRowResponse()
        proto.consumed.capacity_unit.write = 10
        return proto

    def _mock_delete_row(self, body):
        proto = pb2.DeleteRowResponse()
        proto.consumed.capacity_unit.write = 1
        return proto

    def _mock_batch_get_row(self, body):
        proto = pb2.BatchGetRowResponse()

        table_item = proto.tables.add()
        table_item.table_name = 'test_table'

        row_item = table_item.rows.add()
        row_item.is_ok = True
        row_item.consumed.capacity_unit.read = 100

        primary_key_columns = row_item.row.primary_key_columns.add()
        primary_key_columns.name = 'PK1'
        primary_key_columns.value.type = pb2.STRING
        primary_key_columns.value.v_string = 'Hello'
        primary_key_columns = row_item.row.primary_key_columns.add()
        primary_key_columns.name = 'PK2'
        primary_key_columns.value.type = pb2.INTEGER
        primary_key_columns.value.v_int = 100

        column = row_item.row.attribute_columns.add()
        column.name = 'COL1'
        column.value.type = pb2.STRING
        column.value.v_string = 'test'
        column = row_item.row.attribute_columns.add()
        column.name = 'COL2'
        column.value.type = pb2.INTEGER
        column.value.v_int = 1000

        row_item = table_item.rows.add()
        row_item.is_ok = False
        row_item.error.code = 'ErrorCode'
        row_item.error.message = 'ErrorMessage'

        return proto

    def _mock_batch_write_row(self, body):
        proto = pb2.BatchWriteRowResponse()

        table_item = proto.tables.add()
        table_item.table_name = 'test_table'

        put_row_item = table_item.put_rows.add()
        put_row_item.is_ok = True
        put_row_item.consumed.capacity_unit.write = 10

        update_row_item = table_item.update_rows.add()
        update_row_item.is_ok = False
        update_row_item.error.code = 'ErrorCode'
        update_row_item.error.message = 'ErrorMessage'
        update_row_item.consumed.capacity_unit.write = 100
        update_row_item = table_item.update_rows.add()
        update_row_item.is_ok = True
        update_row_item.consumed.capacity_unit.write = 1111

        delete_row_item = table_item.delete_rows.add()
        delete_row_item.is_ok = True
        delete_row_item.consumed.capacity_unit.write = 1000

        return proto

    def _mock_get_range(self, body):
        proto = pb2.GetRangeResponse()

        proto.consumed.capacity_unit.read = 100

        next_primary_key = proto.next_start_primary_key.add()
        next_primary_key.name = 'PK1'
        next_primary_key.value.type = pb2.STRING
        next_primary_key.value.v_string = 'NextStart'
        next_primary_key = proto.next_start_primary_key.add()
        next_primary_key.name = 'PK2'
        next_primary_key.value.type = pb2.INTEGER
        next_primary_key.value.v_int = 101

        row_item = proto.rows.add()
        primary_key = row_item.primary_key_columns.add()
        primary_key.name = 'PK1'
        primary_key.value.type = pb2.STRING
        primary_key.value.v_string = 'Hello'
        primary_key = row_item.primary_key_columns.add()
        primary_key.name = 'PK2'
        primary_key.value.type = pb2.INTEGER
        primary_key.value.v_int = 100

        column = row_item.attribute_columns.add()
        column.name = 'COL1'
        column.value.type = pb2.STRING
        column.value.v_string = 'test'
        column = row_item.attribute_columns.add()
        column.name = 'COL2'
        column.value.type = pb2.INTEGER
        column.value.v_int = 1000

        return proto

    def decode_response(self, api_name, response_body):
        if api_name not in self.api_mock_map:
            raise OTSClientError("No PB decode method for API %s" % api_name)

        handler = self.api_mock_map[api_name]
        return handler(response_body)

