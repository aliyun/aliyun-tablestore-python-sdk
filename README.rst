Aliyun Tablestore SDK for Python
==================================

.. image:: https://badge.fury.io/py/tablestore.svg
    :target: https://badge.fury.io/py/tablestore
.. image:: https://img.shields.io/badge/license-mit-brightgreen.svg
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk
.. image:: https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-python-sdk.svg
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk
.. image:: https://coveralls.io/repos/github/aliyun/aliyun-tablestore-python-sdk/badge.svg?branch=master
    :target: https://coveralls.io/github/aliyun/aliyun-tablestore-python-sdk?branch=master
.. image:: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk.svg
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk

概述
----

- 此 Python SDK 基于 `阿里云表格存储服务 <http://www.aliyun.com/product/ots/>`_  API 构建。
- 阿里云表格存储是构建在阿里云飞天分布式系统之上的 NoSQL 数据存储服务，提供海量结构化数据的存储和实时访问。

运行环境
---------

- 安装 Python 即可运行，支持 python3.8、Python3.9、python3.10、python3.11、python3.12。

安装方法
---------

PIP安装
--------

.. code-block:: bash

    $ pip install tablestore

Github安装
------------

1. 下载源码


.. code-block:: bash

    $ git clone https://github.com/aliyun/aliyun-tablestore-python-sdk.git

2. 安装

.. code-block:: bash

    $ python setup.py install


源码安装
--------

1. 下载 SDK 发布包并解压
2. 安装


.. code-block:: bash

    $ python setup.py install

示例代码
---------

表（Table）示例：

- `表操作（表的创建、获取、更新和删除） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/table_operations.py>`_
- `单行写（向表内写入一行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/put_row.py>`_
- `单行读（从表内读出一样数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/get_row.py>`_
- `更新单行（更新某一行的部分字段） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/update_row.py>`_
- `删除某行（从表内删除某一行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/delete_row.py>`_
- `批量写（向多张表，一次性写入多行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/batch_write_row.py>`_
- `批量读（从多张表，一次性读出多行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/batch_get_row.py>`_
- `范围扫描（给定一个范围，扫描出该范围内的所有数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/get_range.py>`_
- `主键自增列（主键自动生成一个递增ID） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/pk_auto_incr.py>`_
- `全局二级索引 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/secondary_index_operations.py>`_
- `局部事务（提交事务） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/transaction_and_commit.py>`_
- `局部事务（舍弃事务） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/transaction_and_abort.py>`_

多元索引（Search）示例：

- `基础搜索 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/search_index.py>`_
- `并发圈选数据 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/parallel_scan.py>`_
- `全文检索 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/full_text_search.py>`_
- `向量检索 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/parallel_scan.py>`_
- `Max/Min/Sum/Avg/Count/DistinctCount 等 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/agg.py>`_
- `GroupBy/Histogram 等 <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/group_by.py>`_

执行测试
---------

**注意：测试 case 中会有清理某个实例下所有表的动作，所以请使用专门的测试实例来测试。**

1. 测试前准备

.. code-block:: bash

    $ /bin/bash tests_setup.sh

2. 安装nosetests

.. code-block:: bash

    $ pip install nose

3. 设置执行Case的配置

.. code-block:: bash

    $ export OTS_TEST_ACCESS_KEY_ID=<your access key id>
    $ export OTS_TEST_ACCESS_KEY_SECRET=<your access key secret>
    $ export OTS_TEST_ENDPOINT=<tablestore service endpoint>
    $ export OTS_TEST_INSTANCE=<tablestore instance name>

4. 运行case

python3.8、Python3.9、python3.10、python3.11可使用以下命令

.. code-block:: bash

    $ export PYTHONPATH=$(pwd)/tests:$PYTHONPATH; nosetests tests/

python3.12可使用以下命令

.. code-block:: bash

    $ /bin/bash tests_run.sh

编译proto文件
----------------
.. code-block:: bash

    $ /bin/bash protoc.sh

贡献代码
--------
- 我们非常欢迎大家为 Tablestore Python SDK 以及其他 Tablestore SDK 贡献代码。
- 非常感谢 `@Wall-ee <https://github.com/Wall-ee>`_ 对 4.3.0 版本的贡献。

联系我们
--------
- `阿里云 Tablestore 官方网站 <http://www.aliyun.com/product/ots>`_
- `阿里云官网联系方式 <https://help.aliyun.com/document_detail/61890.html>`_
- `阿里云 Tablestore 官方文档 <https://help.aliyun.com/zh/tablestore/product-overview>`_


