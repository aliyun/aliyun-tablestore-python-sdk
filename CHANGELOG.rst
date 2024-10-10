Tablestore SDK for Python 版本记录
===========================

Python SDK 的版本号遵循 `Semantic Versioning <http://semver.org/>`_ 规则。

Version 6.0.1
-------------
- Fix incompatible changes in delete_row

Version 6.0.0
-------------

- Update protobuf from 3.19.0 to 4.25.0
- Support Python 3.8、Python 3.9、Python 3.10、Python 3.11、Python 3.12
- Support Highlight

Version 5.4.3
-------------

- Support SearchIndex Knn Vector Query

Version 5.2.1
-------------

- Optimize SearchResponse

Version 5.2.0
-------------

- Support ParallelScan API
- Support Max/Min/Avg/Sum/Count/DistinctCount
- Support GroupBy API

Version 4.3.5
-------------

- Fix bytearray encode bug

Version 4.3.4
-------------

- replace protobuf-py3 by protobuf

Version 4.3.2
-------------

- remove crcmod

Version 4.3.0
-------------

- Support Python 3.3+

Version 4.2.0
-------------

- Support STS

Version 4.1.0
-------------

- Support Python 2.6

Version 4.0.0
-------------

- 支持主键列自增功能
- 支持多版本
- 支持TTL
- 增大重试时间

Version 2.0.8
-------------

- 支持https访问和证书验证

Version 2.0.7
-------------

- 根据按量计费方式，调整了示例代码中的预留CU设置 

Version 2.0.6
-------------

- 调整了部分异常情况下的重试退避策略

