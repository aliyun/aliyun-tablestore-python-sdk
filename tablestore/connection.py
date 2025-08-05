# -*- coding: utf8 -*-
import ssl
import time

import aiohttp
from aiohttp import ClientTimeout

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

    def __init__(self, host, path, timeout=0, maxsize=50, client_ssl_version=None):
        self.host = host
        self.path = path
       
        self.pool = PoolManager(
            self.NUM_POOLS,
            headers=None,
            cert_reqs='CERT_REQUIRED', # Force certificate check
            ca_certs=certifi.where(),  # Path to the Certifi bundle
            timeout=timeout,
            maxsize=maxsize,
            block=True,
            ssl_version=client_ssl_version
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
        response_headers = dict(response.headers)
        response_body = response.data # TODO figure out why response.read() don't work

        return response.status, response.reason, response_headers, response_body


class AsyncConnectionPool(object):

    def __init__(self, host, path, timeout=50, maxsize=50, keepalive_timeout=12, force_close=False, client_ssl_version=None):
        self.host = host
        self.path = path

        if isinstance(timeout, (list, tuple)):
            conn_timeout, read_timeout = timeout
        else:
            conn_timeout = read_timeout = timeout

        ssl_context = None
        if client_ssl_version is not None:
            ssl_context = ssl.create_default_context()
            ssl_context.minimum_version = client_ssl_version

        self.pool = aiohttp.ClientSession(
            timeout=ClientTimeout(
                sock_connect=conn_timeout,
                sock_read=read_timeout
            ),
            connector=aiohttp.TCPConnector(
                limit=maxsize,
                ssl_context=ssl_context,
                keepalive_timeout=keepalive_timeout,
                force_close=force_close,
            )
        )

    async def send_receive(self, url, request_headers, request_body):

        global _network_io_time

        if _NETWORK_IO_TIME_COUNT_FLAG:
            begin = time.time()

        async with self.pool.request(
            'POST', self.host + self.path + url,
            data=request_body,
            headers=request_headers,
            allow_redirects=False,
        ) as response:
            response_body = await response.read()

        if _NETWORK_IO_TIME_COUNT_FLAG:
            end = time.time()
            _network_io_time += end - begin

        response_headers = dict(response.headers)

        return response.status, response.reason, response_headers, response_body

    async def close(self):
        await self.pool.close()