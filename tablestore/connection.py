# -*- coding: utf8 -*-

import time

try:
    import httplib
except ImportError:
    import http.client

from urllib3.poolmanager import PoolManager
from urllib3.connectionpool import HTTPConnectionPool
import certifi

from tablestore.error import *

_NETWORK_IO_TIME_COUNT_FLAG = False
_network_io_time = 0


class ConnectionPool(object):

    NUM_POOLS = 5    # one pool per host, usually just 1 pool is needed
                      # when redirect happens, one additional pool will be created

    def __init__(self, host, path, timeout=0, maxsize=50):
        self.host = host
        self.path = path
       
        self.pool = PoolManager(
            self.NUM_POOLS,
            headers=None,
            cert_reqs='CERT_REQUIRED', # Force certificate check
            ca_certs=certifi.where(),  # Path to the Certifi bundle
            strict=True,         # TODO more comments to explain these parameters
            timeout=timeout,
            maxsize=maxsize,
            block=True,
        )

    def send_receive(self, url, request_headers, request_body):

        global _network_io_time

        if _NETWORK_IO_TIME_COUNT_FLAG:
            begin = time.time()

        response = self.pool.urlopen(
            'POST', self.host + self.path + url, 
            body=request_body, headers=request_headers,
            redirect=False,
            assert_same_host=False,
        )

        if _NETWORK_IO_TIME_COUNT_FLAG:
            end = time.time()
            _network_io_time += end - begin

        # TODO error handling
        response_headers = dict(response.getheaders())
        response_body = response.data # TODO figure out why response.read() don't work

        return response.status, response.reason, response_headers, response_body
