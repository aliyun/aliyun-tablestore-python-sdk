import os
import ssl

from tablestore import *
from example_config import *

# 以下代码展示如何设置OTSClient使用的TLS版本

access_key_id = OTS_ACCESS_KEY_ID
access_key_secret = OTS_ACCESS_KEY_SECRET

# 创建TLS版本为1.2的连接
ots_client = OTSClient(OTS_ENDPOINT, access_key_id, access_key_secret, OTS_INSTANCE, ssl_version=ssl.PROTOCOL_TLSv1_2)

# do something
resp = ots_client.list_table()
print(resp)
