import os
import ssl

from tablestore import *

# 以下代码展示如何设置OTSClient使用的TLS版本

access_key_id = os.getenv("OTS_AK_ENV")
access_key_secret = os.getenv("OTS_SK_ENV")

# 创建TLS版本为1.2的连接
ots_client = OTSClient('endpoint', access_key_id, access_key_secret, 'instance_name', ssl_version=ssl.PROTOCOL_TLSv1_2)

# do something
resp = ots_client.list_table()
print(resp)
