# -*- coding: utf8 -*-

import hashlib
import base64
import time
import logging
import sys
import platform
import datetime

import google.protobuf.text_format as text_format

import tablestore
import tablestore.utils as utils
from tablestore.auth import SignBase
from tablestore.error import *
from tablestore.encoder import OTSProtoBufferEncoder
from tablestore.decoder import OTSProtoBufferDecoder
import tablestore.protobuf.table_store_pb2 as pb2


class OTSProtocol(object):
    api_version = '2015-12-31'

    if isinstance(sys.version_info, tuple):
        python_version = '%s.%s.%s' % (sys.version_info[0], sys.version_info[1], sys.version_info[2])
    else:
        python_version = '%s.%s.%s' % (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    user_agent = 'aliyun-tablestore-sdk-python/%s(%s/%s/%s;%s)' % (
        tablestore.__version__, platform.system(), platform.release(), platform.machine(), python_version)

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
        'UpdateSearchIndex',
        'DeleteSearchIndex',
        'DescribeSearchIndex',
        'Search',
        'ComputeSplits',
        'ParallelScan',
        'CreateIndex',
        'DropIndex',
        'StartLocalTransaction',
        'CommitTransaction',
        'AbortTransaction',
        'SQLQuery',
        'PutTimeseriesData',
        'CreateTimeseriesTable',
        'ListTimeseriesTable',
        'DeleteTimeseriesTable',
        'DescribeTimeseriesTable',
        'UpdateTimeseriesTable',
        'QueryTimeseriesMeta',
        'GetTimeseriesData',
        'UpdateTimeseriesMeta',
        'DeleteTimeseriesMeta',
    ]

    def __init__(self, instance_name, encoding, logger):
        self.instance_name = instance_name
        self.encoding = encoding
        self.encoder = OTSProtoBufferEncoder(encoding)
        self.decoder = OTSProtoBufferDecoder(encoding)
        self.logger = logger

    def _make_request_headers(self, body, query, signer: SignBase):
        # Compose request headers and process request body if needed.
        # Decode the byte type md5 in order to fit the signature method.
        md5 = base64.b64encode(hashlib.md5(body).digest()).decode(self.encoding)
        header_date = utils.get_now_utc_datetime().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        headers = {
            'x-ots-date': header_date,
            'x-ots-apiversion': self.api_version,
            'x-ots-accesskeyid': signer.get_credentials_provider().get_credentials().get_access_key_id(),
            'x-ots-instancename': self.instance_name,
            'x-ots-contentmd5': md5,
        }
        # extra headers
        sts_token = signer.get_credentials_provider().get_credentials().get_security_token()
        if sts_token is not None:
            headers['x-ots-ststoken'] = sts_token

        signer.make_request_signature_and_add_headers(query, headers)
        headers['User-Agent'] = self.user_agent
        return headers

    @staticmethod
    def _convert_urllib3_headers(headers):
        """
        old urllib3 headers: {'header1':'value1', 'header2':'value2'}
        new urllib3 headers: {'header1':('header1', 'value1'), 'header2':('header2', 'value2')}
        """
        std_headers = {}
        for k, v in headers.items():
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

        if 200 <= status < 300:
            for name in header_names:
                if not name in headers:
                    raise OTSClientError('"%s" is missing in response header.' % name)

        # 2, check md5
        if 'x-ots-contentmd5' in headers:
            # have to decode the byte string inorder to fit the header
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
            now_unix_time = time.mktime(utils.get_now_utc_datetime().timetuple())
            if abs(server_unix_time - now_unix_time) > 15 * 60:
                raise OTSClientError('The difference between date in response and system time is more than 15 minutes.')

    def _check_authorization(self, query, headers, status, signer: SignBase):
        auth = headers.get('authorization')
        if auth is None:
            if 200 <= status < 300:
                raise OTSClientError('"Authorization" is missing in response header.')
            else:
                return

        # 1, check authorization
        if not auth.startswith('OTS '):
            raise OTSClientError('Invalid Authorization in response.')

        # 2, check access key id
        access_id, signature = auth[4:].split(':')
        if access_id != signer.get_credentials_provider().get_credentials().get_access_key_id():
            raise OTSClientError('Invalid access key id in response.')

        # 3, check signature
        # decode the byte type
        if signature != signer.make_response_signature(query, headers):
            raise OTSClientError('Invalid signature in response.')

    def make_request(self, api_name, signer: SignBase, *args, **kwargs):
        if api_name not in self.api_list:
            raise OTSClientError('API %s is not supported.' % api_name)

        proto = self.encoder.encode_request(api_name, *args, **kwargs)
        body = proto.SerializeToString()
        query = '/' + api_name
        headers = self._make_request_headers(body, query, signer)

        if self.logger.level <= logging.DEBUG:
            # prevent to generate formatted message which is time-consuming
            self.logger.debug("OTS request, API: %s, Headers: %s, Protobuf: %s" % (
                api_name, headers,
                text_format.MessageToString(proto, as_utf8=True, as_one_line=True)
            ))
        return query, headers, body

    @staticmethod
    def _get_request_id_string(headers):
        request_id = headers.get('x-ots-requestid')
        if request_id is None:
            request_id = ""
        return request_id

    def parse_response(self, api_name, status, headers, body):
        if api_name not in self.api_list:
            raise OTSClientError("API %s is not supported." % api_name)

        headers = self._convert_urllib3_headers(headers)
        request_id = self._get_request_id_string(headers)

        try:
            ret, proto = self.decoder.decode_response(api_name, body, request_id)
        except Exception as e:
            error_message = 'Response format is invalid, %s, RequestID: %s, " \
                "HTTP status: %s, Body: %s.' % (str(e), request_id, status, body)
            self.logger.error(error_message)
            raise e

        return ret

    def handle_error(self, api_name, query, status, reason, headers, body, signer: SignBase):
        # convert headers according to different urllib3 versions.
        std_headers = self._convert_urllib3_headers(headers)

        if self.logger.level <= logging.DEBUG:
            # prevent to generate formatted message which is time-consuming
            self.logger.debug("OTS response, API: %s, Status: %s, Reason: %s, " \
                              "Headers: %s" % (api_name, status, reason, std_headers))

        if api_name not in self.api_list:
            raise OTSClientError('API %s is not supported.' % api_name)

        try:
            self._check_headers(std_headers, body, status=status)
            if status != 403:
                self._check_authorization(query, std_headers, status=status, signer=signer)
        except OTSClientError as e:
            e.http_status = status
            e.message += " HTTP status: %s." % status
            raise e

        if 200 <= status < 300:
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
                    self._check_authorization(query, std_headers, status=status, signer=signer)
            except OTSClientError as e:
                e.http_status = status
                e.message += " HTTP status: %s." % status
                raise e

            self.logger.error("OTS request failed, API: %s, HTTPStatus: %s, " \
                              "ErrorCode: %s, ErrorMessage: %s, RequestID: %s." % (
                                  api_name, status, error_proto.code, error_proto.message, request_id)
                              )
            raise OTSServiceError(status, error_proto.code, error_proto.message, request_id)
