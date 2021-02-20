# -*- coding: utf8 -*-

import hashlib
import hmac
import base64
import time
import calendar
import logging
import sys
import platform
import datetime
from email.utils import parsedate

try:
    from urlparse import urlparse, parse_qsl
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlparse, parse_qsl, urlencode


import google.protobuf.text_format as text_format

import tablestore
from tablestore.error import *
from tablestore.encoder import OTSProtoBufferEncoder
from tablestore.decoder import OTSProtoBufferDecoder
import tablestore.protobuf.table_store_pb2 as pb2
import tablestore.protobuf.table_store_filter_pb2 as filter_pb2


class OTSProtocol(object):

    api_version = '2015-12-31'

    encoder_class = OTSProtoBufferEncoder
    decoder_class = OTSProtoBufferDecoder

    if isinstance(sys.version_info, tuple):
        python_version = '%s.%s.%s' % (sys.version_info[0], sys.version_info[1], sys.version_info[2])
    else:
        python_version = '%s.%s.%s' % (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    user_agent = 'aliyun-tablestore-sdk-python/%s(%s/%s/%s;%s)' % (tablestore.__version__, platform.system(), platform.release(), platform.machine(), python_version)

    api_list = [
        'CreateTable',
        'ListTable',
        'DeleteTable',
        'DescribeTable',
        'UpdateTable',
        'GetRow',
        'PutRow',
        'UpdateRow',
        'DeleteRow',
        'BatchGetRow',
        'BatchWriteRow',
        'GetRange',
        'ListSearchIndex',
        'CreateSearchIndex',
        'DeleteSearchIndex',
        'DescribeSearchIndex',
        'Search',
        'ComputeSplits',
        'ParallelScan',
        'CreateIndex',
        'DropIndex',
        'StartLocalTransaction',
        'CommitTransaction',
        'AbortTransaction'
    ]

    def __init__(self, user_id, user_key, sts_token, instance_name, encoding, logger):
        self.user_id = user_id
        self.user_key = user_key
        self.sts_token = sts_token
        self.instance_name = instance_name
        self.encoding = encoding
        self.encoder = self.encoder_class(encoding)
        self.decoder = self.decoder_class(encoding)
        self.logger = logger

    def _make_headers_string(self, headers):
        headers_item = ["%s:%s" % (k.lower(), v.strip()) for k, v in headers.items() if k.startswith('x-ots-') and k != 'x-ots-signature']
        return "\n".join(sorted(headers_item))

    def _call_signature_method(self, signature_string):
        # The signature method is supposed to be HmacSHA1
        # A switch case is required if there is other methods available
        signature = base64.b64encode(hmac.new(
            self.user_key.encode(self.encoding), signature_string.encode(self.encoding), hashlib.sha1
        ).digest())
        return signature

    def _make_request_signature(self, query, headers):
        uri, param_string, query_string = urlparse(query)[2:5]

        # TODO a special query should be input to test query sorting,
        # because none of the current APIs uses query map, but the sorting
        # is required in the protocol document.
        query_pairs = parse_qsl(query_string)
        sorted_query = urlencode(sorted(query_pairs))
        signature_string = uri + '\n' + 'POST' + '\n' + sorted_query + '\n'

        headers_string = self._make_headers_string(headers)
        signature_string += headers_string + '\n'
        signature = self._call_signature_method(signature_string)
        return signature

    def _make_headers(self, body, query):
        # compose request headers and process request body if needed

        #decode the byte type md5 in order to fit the signature method
        md5 = base64.b64encode(hashlib.md5(body).digest()).decode(self.encoding)


        date = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')

        headers = {
            'x-ots-date' : date,
            'x-ots-apiversion' : self.api_version,
            'x-ots-accesskeyid' : self.user_id,
            'x-ots-instancename' : self.instance_name,
            'x-ots-contentmd5' : md5,
        }

        if self.sts_token != None:
            headers['x-ots-ststoken'] = self.sts_token

        signature = self._make_request_signature(query, headers)
        headers['x-ots-signature'] = signature
        headers['User-Agent'] = self.user_agent
        return headers

    def _make_response_signature(self, query, headers):
        uri = urlparse(query)[2]
        headers_string = self._make_headers_string(headers)

        signature_string = headers_string + '\n' + uri
        signature = self._call_signature_method(signature_string)
        return signature

    def _convert_urllib3_headers(self, headers):
        """
        old urllib3 headers: {'header1':'value1', 'header2':'value2'}
        new urllib3 headers: {'header1':('header1', 'value1'), 'header2':('header2', 'value2')}
        """
        std_headers = {}
        for k,v in headers.items():
            if isinstance(v, tuple) and len(v) == 2:
                std_headers[k.lower()] = v[1]
            else:
                std_headers[k.lower()] = v

        return std_headers

    def _check_headers(self, headers, body, status=None):
        # check the response headers and process response body if needed.

        # 1, make sure we have all headers
        header_names = [
            'x-ots-contentmd5',
            'x-ots-requestid',
            'x-ots-date',
            'x-ots-contenttype',
        ]

        if status >= 200 and status < 300:
            for name in header_names:
                if not name in headers:
                    raise OTSClientError('"%s" is missing in response header.' % name)

        # 2, check md5
        if 'x-ots-contentmd5' in headers:
            #have to decode the byte string inorder to fit the header
            md5 = base64.b64encode(hashlib.md5(body).digest()).decode(self.encoding)
            if md5 != headers['x-ots-contentmd5']:
                raise OTSClientError('MD5 mismatch in response.')

        # 3, check date
        if 'x-ots-date' in headers:
            try:
                server_time = datetime.datetime.strptime(headers['x-ots-date'], "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                raise OTSClientError('Invalid date format in response.')

            # 4, check date range
            server_unix_time = time.mktime(server_time.timetuple())
            now_unix_time = time.mktime(datetime.datetime.utcnow().timetuple())
            if abs(server_unix_time - now_unix_time) > 15 * 60:
                raise OTSClientError('The difference between date in response and system time is more than 15 minutes.')

    def _check_authorization(self, query, headers, status=None):
        auth = headers.get('authorization')
        if auth is None:
            if status >= 200 and status < 300:
                raise OTSClientError('"Authorization" is missing in response header.')
            else:
                return

        # 1, check authorization
        if not auth.startswith('OTS '):
            raise OTSClientError('Invalid Authorization in response.')

        # 2, check accessid
        access_id, signature = auth[4:].split(':')
        if access_id != self.user_id:
            raise OTSClientError('Invalid accesskeyid in response.')

        # 3, check signature
        # decode the byte type
        if signature != self._make_response_signature(query, headers).decode(self.encoding):
            raise OTSClientError('Invalid signature in response.')

    def make_request(self, api_name, *args, **kwargs):
        if api_name not in self.api_list:
            raise OTSClientError('API %s is not supported.' % api_name)

        proto = self.encoder.encode_request(api_name, *args, **kwargs)
        body = proto.SerializeToString()

        query = '/' + api_name
        headers = self._make_headers(body, query)

        if self.logger.level <= logging.DEBUG:
            # prevent to generate formatted message which is time consuming
            self.logger.debug("OTS request, API: %s, Headers: %s, Protobuf: %s" % (
                api_name, headers,
                text_format.MessageToString(proto, as_utf8=True, as_one_line=True)
            ))

        return query, headers, body

    def _get_request_id_string(self, headers):
        request_id = headers.get('x-ots-requestid')
        if request_id is None:
            request_id = ""
        return request_id

    def parse_response(self, api_name, status, headers, body):
        if api_name not in self.api_list:
            raise OTSClientError("API %s is not supported." % api_name)

        headers = self._convert_urllib3_headers(headers)

        try:
            ret, proto = self.decoder.decode_response(api_name, body)
        except Exception as e:
            request_id = self._get_request_id_string(headers)
            error_message = 'Response format is invalid, %s, RequestID: %s, " \
                "HTTP status: %s, Body: %s.' % (str(e), request_id, status, body)
            self.logger.error(error_message)
            raise e

        if self.logger.level <= logging.DEBUG:
            # prevent to generate formatted message which is time consuming
            request_id = self._get_request_id_string(headers)
            self.logger.debug("OTS response, API: %s, RequestID: %s, Protobuf: %s." % (
                api_name, request_id,
                text_format.MessageToString(proto, as_utf8=True, as_one_line=True)
            ))
        return ret

    def handle_error(self, api_name, query, status, reason, headers, body):
        # convert headers according to different urllib3 versions.
        std_headers = self._convert_urllib3_headers(headers)

        if self.logger.level <= logging.DEBUG:
            # prevent to generate formatted message which is time consuming
            self.logger.debug("OTS response, API: %s, Status: %s, Reason: %s, " \
                "Headers: %s" % (api_name, status, reason, std_headers))

        if api_name not in self.api_list:
            raise OTSClientError('API %s is not supported.' % api_name)


        try:
            self._check_headers(std_headers, body, status=status)
            if status != 403:
                self._check_authorization(query, std_headers, status=status)
        except OTSClientError as e:
            e.http_status = status
            e.message += " HTTP status: %s." % status
            raise e

        if status >= 200 and status < 300:
            return
        else:
            request_id = self._get_request_id_string(std_headers)

            try:
                error_proto = pb2.Error()
                error_proto.ParseFromString(body)
                error_code = error_proto.code
                error_message = error_proto.message
            except:
                error_message = "HTTP status: %s, reason: %s." % (status, reason)
                self.logger.error(error_message)
                raise OTSClientError(error_message, status)

            try:
                if status == 403 and error_proto.code != "OTSAuthFailed":
                    self._check_authorization(query, std_headers)
            except OTSClientError as e:
                e.http_status = status
                e.message += " HTTP status: %s." % status
                raise e

            self.logger.error("OTS request failed, API: %s, HTTPStatus: %s, " \
                "ErrorCode: %s, ErrorMessage: %s, RequestID: %s." % (
                api_name, status, error_proto.code, error_proto.message, request_id)
            )
            raise OTSServiceError(status, error_proto.code, error_proto.message, request_id)
