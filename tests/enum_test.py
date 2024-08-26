# -*- coding: utf8 -*-

from tests.lib.api_test_base import APITestBase
from tablestore import metadata
from tablestore.protobuf import search_pb2
from enum import IntEnum
import inspect

class EnumTest(APITestBase):
    def setUp(self):
        pass # no need to set up client
    
    def tearDown(self):
        pass # no need to tearDown client
    
    def test_IntEnum_equal_int(self):
        self.assert_equal(metadata.HighlightEncoder.PLAIN_MODE, search_pb2.PLAIN_MODE)
        self.assert_equal(metadata.HighlightEncoder.HTML_MODE, search_pb2.HTML_MODE)
