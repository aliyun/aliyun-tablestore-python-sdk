# -*- coding: utf8 -*-

import six
import struct
from builtins import int
from tablestore.error import *
from .plain_buffer_consts import *

class PlainBufferInputStream(object):
    def __init__(self, data_buffer):
        self.buffer = data_buffer
        self.len = len(self.buffer)
        self.cur_pos = 0
        self.last_tag = chr(0)

    def read_tag(self):
        if len(self.buffer) == self.cur_pos:
            self.last_tag = 0
        else:
            self.last_tag = self.read_raw_byte()

    def check_last_tag_was(self, tag):
        return self.last_tag == tag

    def get_last_tag(self):
        return self.last_tag

    def read_raw_byte(self):
        if self.len != self.cur_pos:
            pos = self.cur_pos
            self.cur_pos += 1
            if const.python_version == 2:
                return self.buffer[pos]
            else:
                return chr(self.buffer[pos])
        else:
            raise OTSClientError("Read raw byte encountered EOF.")

    def read_raw_little_endian64(self):
        return struct.unpack('<q', self.read_bytes(8))[0]

    def read_raw_little_endian32(self):
        return struct.unpack('<i', self.read_bytes(4))[0]
    
    def read_boolean(self):
        return struct.unpack('<?', self.read_bytes(1))[0]

    def read_double(self):
        return struct.unpack('<q', self.read_bytes(8))[0]

    def read_int32(self):
        return struct.unpack('<i', self.read_bytes(4))[0]

    def read_int64(self):
        return struct.unpack('<q', self.read_bytes(8))[0]

    def read_bytes(self, size):
        if self.len - self.cur_pos < size:
            raise OTSClientError("Read bytes encountered EOF.")

        tmp_pos = self.cur_pos
        self.cur_pos += size
        return self.buffer[tmp_pos: tmp_pos + size]

    def read_utf_string(self, size):
        if self.len - self.cur_pos < size:
            raise OTSClientError("Read UTF string encountered EOF.")
        utf_str = self.buffer[self.cur_pos:self.cur_pos + size]
        self.cur_pos += size
        if isinstance(utf_str, six.binary_type):
            utf_str = utf_str.decode('utf-8')
        return utf_str

        
class PlainBufferOutputStream(object):
    def __init__(self, capacity):
        self.buffer = bytearray()
        self.capacity = capacity

    def get_buffer(self):
        return self.buffer

    def is_full(self):
        return self.capacity<= len(self.buffer)

    def count(self):
        return len(self.buffer)

    def remain(self):
        return self.capacity - self.count()

    def clear(self):
        self.buffer = bytearray('')

    def write_raw_byte(self, value):
        if self.is_full():
            raise OTSClientError("The buffer is full")
        self.buffer.append(value)

    def write_raw_little_endian32(self, value):
        self.write_bytes(struct.pack("i", value))

    def write_raw_little_endian64(self, value):
        self.write_bytes(struct.pack("q", value))
        
    def write_double(self, value):
        self.write_bytes(struct.pack('d', value))

    def write_boolean(self, value):
        self.write_bytes(struct.pack('?', value))

    def write_bytes(self, value):
        if len(self.buffer) + len(value) > self.capacity:
            raise OTSClientError("The buffer is full.")
        if isinstance(value, six.text_type):
            value = value.encode('utf-8')
        self.buffer += bytearray(value)
