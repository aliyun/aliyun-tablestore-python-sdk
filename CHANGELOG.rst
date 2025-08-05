Tablestore SDK for Python 版本记录
===========================

Python SDK 的版本号遵循 `Semantic Versioning <http://semver.org/>`_ 规则。

Version 6.3.0
-------------
- Support async client
- Add async and sync mixed test by RandomOTSClient in tests/__init__.py

Version 6.2.1
-------------
- Support SingleColumnRegexCondition filter

Version 6.2.0
-------------
- 支持无 AK 方案，OTSClient 支持 credentials_provider 参数。
- 使用 Poetry 管理项目，提高开发和发布效率。
- Flatc 实时编译 fbs 文件。
- 解决 dataprotocol 包不在 tablestore 项目下面的问题，避免包冲突。
- 优化 ut 测试。

Version 6.1.0
-------------
- Support some timeseries api.
- Update protobuf to ">=3.20.0,<=5.27.4"
- Refine util shell 'protoc.sh'

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

