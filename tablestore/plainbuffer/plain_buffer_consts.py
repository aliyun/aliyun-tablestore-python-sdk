# -*- coding: utf8 -*-

import sys
import platform
import tablestore.const as const

const.python_version = sys.version_info[0]

const.HEADER = 0x75

# tag type
const.TAG_ROW_PK = chr(0x1)
const.TAG_ROW_DATA = chr(0x2)
const.TAG_CELL = chr(0x3)
const.TAG_CELL_NAME = chr(0x4)
const.TAG_CELL_VALUE = chr(0x5)
const.TAG_CELL_TYPE = chr(0x6)
const.TAG_CELL_TIMESTAMP = chr(0x7)
const.TAG_DELETE_ROW_MARKER = chr(0x8)
const.TAG_ROW_CHECKSUM = chr(0x9)
const.TAG_CELL_CHECKSUM = chr(0x0A)

# cell op type
const.DELETE_ALL_VERSION = 0x1
const.DELETE_ONE_VERSION = 0x3
const.INCREMENT = 0x4

# variant type
const.VT_INTEGER = 0x0
const.VT_DOUBLE = 0x1
const.VT_BOOLEAN = 0x2
const.VT_STRING = 0x3
const.VT_NULL = 0x6
const.VT_BLOB = 0x7
const.VT_INF_MIN = 0x9
const.VT_INF_MAX = 0xa
const.VT_AUTO_INCREMENT = 0xb

# othber
const.LITTLE_ENDIAN_32_SIZE = 4
const.LITTLE_ENDIAN_64_SIZE = 8
const.MAX_BUFFER_SIZE = 64 * 1024 * 1024

const.SYS_BITS = int(platform.architecture()[0][:2])
if const.SYS_BITS == 64:
    const.LITTLE_ENDIAN_SIZE = const.LITTLE_ENDIAN_64_SIZE
elif const.SYS_BITS == 32:
    const.LITTLE_ENDIAN_SIZE = const.LITTLE_ENDIAN_32_SIZE
else:
    const.LITTLE_ENDIAN_SIZE = 4


