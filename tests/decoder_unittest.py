# -*- coding: utf8 -*-

import unittest


from tests.lib.api_test_base import APITestBase
from tablestore import decoder


class DecoderTest(APITestBase):


    """DecoderTest"""

    def test_decode_timeseries_tag_or_attribute(self):
        d = decoder.OTSProtoBufferDecoder("utf-8")

        attri = d._parse_timeseries_tag_or_attribute("[]")
        self.assert_equal(len(attri), 0)

        attri = d._parse_timeseries_tag_or_attribute("[\"a=a1\",\"b=b2\",\"c=0.3\"]")
        self.assert_equal(len(attri), 3)
        try:
            d._parse_timeseries_tag_or_attribute("[a=a1\",\"b=b2\",\"c=0.3\"]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a==a1\",\"b=b2\",\"c=0.3\"]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a==a1\",\"b=b2=0.3\"]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a=a1\",]")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)

        try:
            d._parse_timeseries_tag_or_attribute("[\"a=a1\"")
            self.fail("should have failed")
        except Exception as e:
            self.assertTrue(e is not None)


if __name__ == '__main__':
    unittest.main()