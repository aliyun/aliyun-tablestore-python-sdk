# -*- coding: utf8 -*-

import hashlib
import hmac
import base64
import six
from abc import ABC, abstractmethod

try:
    from urlparse import urlparse, parse_qsl
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlparse, parse_qsl, urlencode

import tablestore.consts as consts
import tablestore.utils as utils
from tablestore.credentials import CredentialsProvider
from tablestore.error import *


def calculate_hmac(signing_key, signature_string, sign_method, encoding):
    if isinstance(signing_key, six.text_type):
        signing_key = signing_key.encode(encoding)
    if isinstance(signature_string, six.text_type):
        signature_string = signature_string.encode(encoding)
    return hmac.new(signing_key, signature_string, sign_method).digest()


def call_signature_method_sha1(signing_key, signature_string, encoding):
    # The signature method is supposed to be HmacSHA1
    return base64.b64encode(calculate_hmac(signing_key, signature_string, hashlib.sha1, encoding)).decode(encoding)


def call_signature_method_sha256(signing_key, signature_string, encoding):
    # The signature method is supposed to be HmacSHA256
    return base64.b64encode(calculate_hmac(signing_key, signature_string, hashlib.sha256, encoding)).decode(encoding)


class SignBase(ABC):
    def __init__(self, credentials_provider: CredentialsProvider, encoding, **kwargs):
        self.credentials_provider = credentials_provider
        self.encoding = encoding
        self.signing_key = None

    def get_credentials_provider(self):
        return self.credentials_provider

    @staticmethod
    def _make_headers_string(headers):
        headers_item = ["%s:%s" % (k.lower(), v.strip()) for k, v in headers.items()
                        if k.startswith(consts.OTS_HEADER_PREFIX)]
        return "\n".join(sorted(headers_item))

    def _get_request_signature_string(self, query, headers):
        uri, param_string, query_string = urlparse(query)[2:5]

        # TODO a special query should be input to test query sorting,
        # because none of the current APIs uses query map, but the sorting
        # is required in the protocol document.
        query_pairs = parse_qsl(query_string)
        sorted_query = urlencode(sorted(query_pairs))
        signature_string = uri + '\n' + 'POST' + '\n' + sorted_query + '\n'

        headers_string = self._make_headers_string(headers)
        signature_string += headers_string + '\n'
        return signature_string

    def make_response_signature(self, query, headers):
        uri = urlparse(query)[2]
        headers_string = self._make_headers_string(headers)
        signature_string = headers_string + '\n' + uri
        # Response signature use same signing key as request signature
        # But the signature method is supposed to be HmacSHA1
        signature = call_signature_method_sha1(self.signing_key, signature_string, self.encoding)
        return signature

    @abstractmethod
    def gen_signing_key(self):
        pass

    @abstractmethod
    def make_request_signature_and_add_headers(self, query, headers):
        pass


class SignV2(SignBase):
    def __init__(self, credentials_provider: CredentialsProvider, encoding, **kwargs):
        SignBase.__init__(self, credentials_provider, encoding, **kwargs)

    def gen_signing_key(self):
        self.signing_key = self.credentials_provider.get_credentials().get_access_key_secret()

    def make_request_signature_and_add_headers(self, query, headers):
        signature_string = self._get_request_signature_string(query, headers)
        headers[consts.OTS_HEADER_SIGNATURE] = call_signature_method_sha1(self.signing_key, signature_string,
                                                                          self.encoding)


class SignV4(SignBase):
    def __init__(self, credentials_provider: CredentialsProvider, encoding, **kwargs):
        SignBase.__init__(self, credentials_provider, encoding, **kwargs)
        self.user_key = None
        self.region = kwargs.get('region')
        if not isinstance(self.region, str) or self.region == '':
            raise OTSClientError('region is not str or is empty.')
        self.sign_date = kwargs.get('sign_date')
        self.auto_update_v4_sign = (kwargs.get('auto_update_v4_sign') is True)
        if self.sign_date is None:
            self.sign_date = utils.get_now_utc_datetime().strftime(consts.V4_SIGNATURE_SIGN_DATE_FORMAT)
            self.auto_update_v4_sign = True

    def gen_signing_key(self):
        # if the signing_key is None, we need to update signing_key.
        need_update = self.signing_key is None
        # if the user_key changes, we need to update signing_key.
        cur_user_key = self.credentials_provider.get_credentials().get_access_key_secret()
        if cur_user_key != self.user_key:
            self.user_key = cur_user_key
            need_update = True
        # for v4, only update the sign date and signing_key
        if self.auto_update_v4_sign:
            cur_date = utils.get_now_utc_datetime().strftime(consts.V4_SIGNATURE_SIGN_DATE_FORMAT)
            # if sign_date changes, we need to update signing_key.
            if cur_date != self.sign_date:
                self.sign_date = cur_date
                need_update = True
        if self.sign_date is None:
            raise OTSClientError('v4 sign_date is None.')
        if not need_update:
            return
        origin_signing_key = consts.V4_SIGNATURE_PREFIX + self.user_key
        first_signing_key = calculate_hmac(origin_signing_key, self.sign_date, hashlib.sha256, self.encoding)
        second_signing_key = calculate_hmac(first_signing_key, self.region, hashlib.sha256, self.encoding)
        third_signing_key = calculate_hmac(second_signing_key, consts.V4_SIGNATURE_PRODUCT, hashlib.sha256,
                                           self.encoding)
        fourth_signing_key = calculate_hmac(third_signing_key, consts.V4_SIGNATURE_CONSTANT, hashlib.sha256,
                                            self.encoding)
        self.signing_key = base64.b64encode(fourth_signing_key)

    def make_request_signature_and_add_headers(self, query, headers):
        headers[consts.OTS_HEADER_SIGN_DATE] = self.sign_date
        headers[consts.OTS_HEADER_SIGN_REGION] = self.region
        signature_string = self._get_request_signature_string(query, headers)
        signature_string += consts.V4_SIGNATURE_SALT
        headers[consts.OTS_HEADER_SIGNATURE_V4] = call_signature_method_sha256(self.signing_key, signature_string,
                                                                               self.encoding)
