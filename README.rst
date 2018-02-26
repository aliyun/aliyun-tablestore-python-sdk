Aliyun TableStore SDK for Python
==================================

.. image:: https://img.shields.io/badge/license-apache2-brightgreen.svg
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk
.. image:: https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-python-sdk.svg
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk
.. image:: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk.svg
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk

概述
----

- 此Python SDK基于 `阿里云表格存储服务 <http://www.aliyun.com/product/ots/>`_  API构建。
- 阿里云表格存储是构建在阿里云飞天分布式系统之上的NoSQL数据存储服务，提供海量结构化数据的存储和实时访问。

运行环境
---------

- 安装Python即可运行，支持python2.6、Python2.7、python3.3、python3.4、python3.5和python3.6。

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

1. 下载SDK发布包并解压
2. 安装


.. code-block:: bash

    $ python setup.py install

示例代码
---------

- `表操作（表的创建、获取、更新和删除） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/table_operations.py>`_
- `单行写（向表内写入一行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/put_row.py>`_
- `单行读（从表内读出一样数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/get_row.py>`_
- `更新单行（更新某一行的部分字段） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/update_row.py>`_
- `删除某行（从表内删除某一行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/delete_row.py>`_
- `批量写（向多张表，一次性写入多行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/batch_write_row.py>`_
- `批量读（从多张表，一次性读出多行数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/batch_get_row.py>`_
- `范围扫描（给定一个范围，扫描出该范围内的所有数据） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/get_range.py>`_
- `主键自增列（主键自动生成一个递增ID） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/pk_auto_incr.py>`_

执行测试
---------

**注意：测试case中会有清理某个实例下所有表的动作，所以请使用专门的测试实例来测试。**

1. 安装nosetests

.. code-block:: bash

    $ pip install nose

2. 设置执行Case的配置

.. code-block:: bash

    $ export OTS_TEST_ACCESS_KEY_ID=<your access id>
    $ export OTS_TEST_ACCESS_KEY_SECRET=<your access key>
    $ export OTS_TEST_ENDPOINT=<tablestore service endpoint>
    $ export OTS_TEST_INSTANCE=<your instance name>

2. 运行case

.. code-block:: bash

    $ nosetests tests/

贡献代码
--------
- 我们非常欢迎大家为TableStore Python SDK以及其他TableStore SDK贡献代码。
- 感谢 `@Wall-ee <https://github.com/Wall-ee>`_ 对4.3.0版本的贡献。

联系我们
--------
- `阿里云TableStore官方网站 <http://www.aliyun.com/product/ots>`_
- `阿里云官网联系方式 <https://help.aliyun.com/document_detail/61890.html>`_
