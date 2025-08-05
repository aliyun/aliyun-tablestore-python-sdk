# -*- coding: utf8 -*-
# Implementation of OTSClient

__all__ = ['OTSClient', 'AsyncOTSClient']

import asyncio
import time
import logging
from abc import ABC

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from tablestore.credentials import CredentialsProvider, StaticCredentialsProvider
from tablestore.auth import SignV2, SignV4
from tablestore.protocol import OTSProtocol
from tablestore.connection import ConnectionPool, AsyncConnectionPool
from tablestore.metadata import *
from tablestore.retry import DefaultRetryPolicy


class BaseOTSClient(ABC):

    DEFAULT_ENCODING = 'utf8'
    DEFAULT_SOCKET_TIMEOUT = 50
    DEFAULT_MAX_CONNECTION = 50
    DEFAULT_LOGGER_NAME = 'tablestore-client'

    def __init__(self, end_point, access_key_id=None, access_key_secret=None, instance_name=None,
                 credentials_provider: CredentialsProvider = None, region: str = None, **kwargs):
        """
        Initialize an ``OTSClient`` instance.

        ``end_point`` is the address of the OTS service (e.g., 'https://instance.cn-hangzhou.ots.aliyun.com'), and must start with 'http://' or 'https://'.

        ``access_key_id`` is the accessid for accessing the OTS service, which can be obtained through the official website or from the administrator.

        ``access_key_secret`` is the accesskey for accessing the OTS service, which can be obtained through the official website or from the administrator.

        ``instance_name`` is the name of the instance to be accessed, which can be created via the official website console or obtained from the administrator.

        ``sts_token`` is the STS token for accessing the OTS service, obtained from the STS service. It has a validity period and needs to be re-obtained after expiration.

        ``credentials_provider`` is the user credential provider for accessing the OTS service, which can provide parameters such as access_key and sts_token.

        ``region`` is the region where the OTS service is located. If it is not empty, v4 signing will be used.

        ``sign_date`` is the date used for signing when using v4 signing. The default value is the UTC date of the current day.

        ``auto_update_v4_sign`` specifies whether to automatically update the signing date when using v4 signing.

        ``encoding`` is the string encoding type for request parameters. The default is utf8.

        ``socket_timeout`` is the Socket timeout for each connection in the connection pool, measured in seconds. It can be an int or float. The default value is 50.

        ``max_connection`` is the maximum number of connections in the connection pool. The default is 50.

        ``logger_name`` is used to log DEBUG logs during requests or ERROR logs when errors occur.

        ``retry_policy`` defines the retry policy. The default retry policy is DefaultRetryPolicy. You can inherit from RetryPolicy to implement your own retry policy; please refer to the code of DefaultRetryPolicy.

        ``ssl_version`` defines the TLS version used for https connections. The default is None.


        Example: Create an OTSClient instance

            from tablestore.client import OTSClient

            client = OTSClient('your_instance_endpoint', 'your_user_id', 'your_user_key', 'your_instance_name', region='region')
        """
        # initialize credentials provider
        self.credentials_provider = self._create_credentials_provider(
            end_point=end_point,
            instance_name=instance_name,
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            sts_token=kwargs.get('sts_token'),
            credentials_provider=credentials_provider
        )
        self.encoding = kwargs.get('encoding')
        if self.encoding is None:
            self.encoding = OTSClient.DEFAULT_ENCODING
        if region is None:
            self._signer = SignV2(self.credentials_provider, self.encoding)
        else:
            self._signer = SignV4(
                self.credentials_provider,
                self.encoding,
                region=region,
                **kwargs
            )

        # initialize logger
        logger_name = kwargs.get('logger_name')
        if logger_name is None:
            self.logger = logging.getLogger(OTSClient.DEFAULT_LOGGER_NAME)
            null_handler = NullHandler()
            self.logger.addHandler(null_handler)
        else:
            self.logger = logging.getLogger(logger_name)

        # parse end point
        scheme, netloc, path = urlparse.urlparse(end_point)[:3]
        host = scheme + "://" + netloc

        if scheme != 'http' and scheme != 'https':
            raise OTSClientError(
                "protocol of end_point must be 'http' or 'https', e.g. https://instance.cn-hangzhou.ots.aliyun.com."
            )
        if host == '':
            raise OTSClientError(
                "host of end_point should be specified, e.g. https://instance.cn-hangzhou.ots.aliyun.com."
            )

        # initialize protocol instance via user configuration
        self.protocol = OTSProtocol(
            instance_name=instance_name,
            encoding=self.encoding,
            logger=self.logger
        )

        # initialize connection via user configuration
        self.socket_timeout = kwargs.get('socket_timeout')
        if self.socket_timeout is None:
            self.socket_timeout = OTSClient.DEFAULT_SOCKET_TIMEOUT
        self.max_connection = kwargs.get('max_connection')
        if self.max_connection is None:
            self.max_connection = OTSClient.DEFAULT_MAX_CONNECTION
        self.ssl_version = kwargs.get('ssl_version')

        self.host = host
        self.path = path

        # initialize the retry policy
        retry_policy = kwargs.get('retry_policy')
        if retry_policy is None:
            retry_policy = DefaultRetryPolicy()
        self.retry_policy = retry_policy

    @staticmethod
    def _create_credentials_provider(end_point, instance_name, access_key_id, access_key_secret,
                                     sts_token: str = None,
                                     credentials_provider: CredentialsProvider = None) -> CredentialsProvider:
        if not isinstance(end_point, str) or end_point == '':
            raise OTSClientError('end_point is not str or is empty.')
        if not isinstance(instance_name, str) or instance_name == '':
            raise OTSClientError('instance_name is not str or is empty.')
        if credentials_provider is None:
            if not isinstance(access_key_id, str) or access_key_id == '':
                raise OTSClientError('access_key_id is not str or is empty.')
            if not isinstance(access_key_secret, str) or access_key_secret == '':
                raise OTSClientError('access_key_secret is not str or is empty.')
            return StaticCredentialsProvider(access_key_id=access_key_id, access_key_secret=access_key_secret,
                                             security_token=sts_token)
        else:
            return credentials_provider

class OTSClient(BaseOTSClient):
    """
    `OTSClient` implements all the interfaces of the OTS service. Users can create an instance of `OTSClient` and call its
    methods to access all features of the OTS service. Users can set various permissions, connection parameters, etc., in the initialization method `__init__()`.

    Unless otherwise stated, all interfaces of `OTSClient` handle errors by throwing exceptions (please refer to the `tablestore.error` module).
    That is, if a function has a return value, it will be described in the documentation; otherwise, it returns None.
    """

    def __init__(self, end_point, access_key_id=None, access_key_secret=None, instance_name=None,
                 credentials_provider: CredentialsProvider = None, region: str = None, **kwargs):
        super().__init__(
            end_point=end_point,
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            instance_name=instance_name,
            credentials_provider=credentials_provider,
            region=region,
            **kwargs,
        )
        self.connection = ConnectionPool(
            self.host, self.path, timeout=self.socket_timeout, maxsize=self.max_connection, client_ssl_version=self.ssl_version
        )

    def _request_helper(self, api_name, *args, **kwargs):
        # Generate signing key, each request generate once
        # Must generate before making request headers
        self._signer.gen_signing_key()
        query, req_headers, req_body = self.protocol.make_request(api_name, self._signer, *args, **kwargs)

        retry_times = 0
        while True:
            try:
                status, reason, res_headers, res_body = self.connection.send_receive(query, req_headers, req_body)
                self.protocol.handle_error(api_name, query, status, reason, res_headers, res_body, self._signer)
                break
            except OTSServiceError as e:
                if self.retry_policy.should_retry(retry_times, e, api_name):
                    retry_delay = self.retry_policy.get_retry_delay(retry_times, e, api_name)
                    time.sleep(retry_delay)
                    retry_times += 1
                else:
                    raise e

        return self.protocol.parse_response(api_name, status, res_headers, res_body)

    def create_table(self, table_meta, table_options, reserved_throughput, secondary_indexes=None):
        """
        Description: Creates a table based on the table information.

        ``table_meta`` is an instance of the ``tablestore.metadata.TableMeta`` class. It includes the table name and the schema of the PrimaryKey.
        Refer to the documentation for the ``TableMeta`` class. After creating a table, it usually takes about 1 minute for partition loading to complete before various operations can be performed.
        ``table_options`` is an instance of the ``tablestore.metadata.TableOptions`` class, which includes three parameters: time_to_live, max_version, and max_time_deviation.
        ``reserved_throughput`` is an instance of the ``tablestore.metadata.ReservedThroughput`` class, representing the reserved read/write throughput.
        ``secondary_indexes`` is an array that can include one or more instances of the ``tablestore.metadata.SecondaryIndexMeta`` class, representing the secondary indexes to be created.

        Return: None.

        Example:

            schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
            table_meta = TableMeta('myTable', schema_of_primary_key)
            table_options = TableOptions()
            reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
            client.create_table(table_meta, table_options, reserved_throughput)
        """

        if secondary_indexes is None:
            secondary_indexes = []
        self._request_helper('CreateTable', table_meta, table_options, reserved_throughput, secondary_indexes)

    def delete_table(self, table_name):
        """
        Description: Delete a table according to the table name.

        ``table_name`` is the corresponding table name.

        Return: None.

        Example:

            client.delete_table('myTable')
        """

        self._request_helper('DeleteTable', table_name)

    def list_table(self):
        """
        Description: Get a list of all table names.

        Return: A list of table names.

        ``table_list`` represents the list of table names obtained, which is of type tuple, e.g., ('MyTable1', 'MyTable2').

        Example:

            table_list = client.list_table()
        """

        return self._request_helper('ListTable')

    def update_table(self, table_name, table_options=None, reserved_throughput=None):
        """
        Description: Update table properties, currently only supports modifying the reserved read/write throughput.

        ``table_name`` is the corresponding table name.
        ``table_options`` is an instance of the ``tablestore.metadata.TableOptions`` class, which includes three parameters: time_to_live, max_version, and max_time_deviation.
        ``reserved_throughput`` is an instance of the ``tablestore.metadata.ReservedThroughput`` class, representing the reserved read/write throughput.

        Return: The most recent increase time, decrease time, and the number of decreases on the same day for the reserved read/write throughput of this table.

        ``update_table_response`` represents the result of the update, which is an instance of the ``tablestore.metadata.UpdateTableResponse`` class.

        Example:

            reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
            table_options = TableOptions();
            update_response = client.update_table('myTable', table_options, reserved_throughput)
        """

        return self._request_helper(
            'UpdateTable', table_name, table_options, reserved_throughput
        )

    def describe_table(self, table_name):
        """
        Description: Get the description information of the table.

        ``table_name`` is the corresponding table name.

        Return: The description information of the table.

        ``describe_table_response`` represents the description information of the table, which is an instance of the tablestore.metadata.DescribeTableResponse class.

        Example:

            describe_table_response = client.describe_table('myTable')
        """

        return self._request_helper('DescribeTable', table_name)

    def get_row(self, table_name, primary_key, columns_to_get=None,
                column_filter=None, max_version=1, time_range=None,
                start_column=None, end_column=None, token=None,
                transaction_id=None):
        """
        Description: Get a single row of data.

        ``table_name`` is the corresponding table name.
        ``primary_key`` is the primary key, with a type of dict.
        ``columns_to_get`` is an optional parameter, representing a list of column names to retrieve, with a type of list; if not specified, it retrieves all columns.
        ``column_filter`` is an optional parameter, indicating a filter for reading rows based on specific conditions.
        ``max_version`` is an optional parameter, indicating the maximum number of versions to read.
        ``time_range`` is an optional parameter, indicating the version range or specific version to read, and at least one of ``time_range`` or ``max_version`` must be provided.

        Return: The consumed CapacityUnit for this operation, primary key columns, and attribute columns.

        ``consumed`` indicates the consumed CapacityUnit, which is an instance of the tablestore.metadata.CapacityUnit class.
        ``return_row`` indicates the row data, including primary key columns and attribute columns, both of type list, such as: [('PK0', value0), ('PK1', value1)].
        ``next_token`` indicates the position for the next read when reading wide rows, encoded as binary.

        Example:

            primary_key = [('gid', 1), ('uid', 101)]
            columns_to_get = ['name', 'address', 'age']
            consumed, return_row, next_token = client.get_row('myTable', primary_key, columns_to_get)
        """

        return self._request_helper(
            'GetRow', table_name, primary_key, columns_to_get,
            column_filter, max_version, time_range,
            start_column, end_column, token, transaction_id
        )

    def put_row(self, table_name, row, condition=None, return_type=None, transaction_id=None):
        """
        Description: Write a row of data. Returns the CapacityUnit consumed by this operation.

        ``table_name`` is the corresponding table name.
        ``row`` is the row data, including the primary key and attribute columns.
        ``condition`` indicates a condition check to be performed before executing the operation; the operation will only execute if the condition is met. It is an instance of the tablestore.metadata.Condition class.
        Currently, two types of condition checks are supported: one is a check on the existence of the row, with possible conditions including 'IGNORE', 'EXPECT_EXIST', and 'EXPECT_NOT_EXIST'; the other is a condition check on the value of attribute columns.
        ``return_type`` indicates the return type, which is an instance of the tablestore.metadata.ReturnType class. Currently, it only supports returning the PrimaryKey, typically used in scenarios involving auto-increment of primary key columns.

        Return: The CapacityUnit consumed by this operation and the requested row data.

        ``consumed`` represents the consumed CapacityUnit, which is an instance of the tablestore.metadata.CapacityUnit class.
        ``return_row`` represents the returned row data, which may include the primary key and attribute columns.

        Example:

            primary_key = [('gid',1), ('uid',101)]
            attribute_columns = [('name','张三'), ('mobile',111111111), ('address','中国A地'), ('age',20)]
            row = Row(primary_key, attribute_columns)
            condition = Condition('EXPECT_NOT_EXIST')
            consumed, return_row = client.put_row('myTable', row, condition)
        """

        return self._request_helper(
            'PutRow', table_name, row, condition, return_type, transaction_id
        )

    def update_row(self, table_name, row, condition, return_type=None, transaction_id=None):
        """
        Description: Update a row of data.

        ``table_name`` is the corresponding table name.
        ``row`` represents the updated row data, including primary key columns and attribute columns. Primary key columns are lists; attribute columns are dictionaries.
        ``condition`` indicates performing a condition check before executing the operation, and the operation will only be executed if the condition is met. It is an instance of the tablestore.metadata.Condition class.
        Currently, two types of condition checks are supported: one is checking the existence of the row, with check conditions including 'IGNORE', 'EXPECT_EXIST', and 'EXPECT_NOT_EXIST'; the other is condition checks on the values of attribute columns.
        ``return_type`` represents the return type, which is an instance of the tablestore.metadata.ReturnType class. Currently, it only supports returning the PrimaryKey, generally used for auto-increment in primary key columns.

        Return: The CapacityUnit consumed by this operation and the row data to be returned (return_row).

        consumed represents the CapacityUnit consumed, which is an instance of the tablestore.metadata.CapacityUnit class.
        return_row represents the row data to be returned.

        Example:

            primary_key = [('gid',1), ('uid',101)]
            update_of_attribute_columns = {
                'put' : [('name','Zhang Sanfeng'), ('address','Location B, China')],
                'delete' : [('mobile', 1493725896147)],
                'delete_all' : [('age')],
                'increment' : [('counter', 1)]
            }
            row = Row(primary_key, update_of_attribute_columns)
            condition = Condition('EXPECT_EXIST')
            consumed = client.update_row('myTable', row, condition)
        """

        return self._request_helper(
            'UpdateRow', table_name, row, condition, return_type, transaction_id
        )

    def delete_row(self, table_name, row=None, condition=None, return_type=None, transaction_id=None, **kwargs):
        """
        Description: Delete a row of data.

        ``table_name`` is the corresponding table name.
        ``row`` represents the primary key.
        ``condition`` indicates a condition check performed before the operation, which executes only if the condition is met. It is an instance of the tablestore.metadata.Condition class.
        Currently, two types of condition checks are supported: one checks the existence of the row, with possible conditions including 'IGNORE', 'EXPECT_EXIST', and 'EXPECT_NOT_EXIST'; the other performs a condition check on the value of attribute columns.

        Return: The CapacityUnit consumed by this operation and the row data to be returned (`return_row`).

        `consumed` indicates the consumed CapacityUnit, which is an instance of the tablestore.metadata.CapacityUnit class.
        `return_row` indicates the row data to be returned.

        Example:

            primary_key = [('gid',1), ('uid',101)]
            row = Row(primary_key)
            condition = Condition('IGNORE')
            consumed, return_row = client.delete_row('myTable', row, condition)
        """

        primary_key = kwargs.get('primary_key', None)
        # When row is not empty, the row parameter will be used preferentially, but the primary_key parameter is required for passing parameters.
        if row is not None:
            primary_key = row
        # When passing Row as a parameter, extract the primary_key
        if isinstance(primary_key, Row):
            primary_key = primary_key.primary_key
        return self._request_helper(
            'DeleteRow', table_name, primary_key, condition, return_type, transaction_id
        )

    def exe_sql_query(self, query):
        """
        Description: Executes an SQL query.

        ``query`` is the query to be executed.
        
        (rows, table_capacity_units, search_capacity_units)

        Returns:
        ``table_capacity_units``  The CapacityUnit consumed for each table by this operation
        ``search_capacity_units`` The CapacityUnit consumed for each search by this operation
        ``rows``                  The returned data
        
        Example:
        row_list, table_consume_list, search_consume_list = client.exe_sql_query(query)
        """
        return self._request_helper(
            'SQLQuery', query
        )

    def batch_get_row(self, request):
        """
        Description: Batch retrieve multiple rows of data.
        request = BatchGetRowRequest()

        request.add(TableInBatchGetRowItem(myTable0, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable1, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable2, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable3, primary_keys, column_to_get=None, column_filter=None))

        response = client.batch_get_row(request)

        ``response`` is the returned result, of type tablestore.metadata.BatchGetRowResponse

        Example:
            cond = CompositeColumnCondition(LogicalOperator.AND)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

            request = BatchGetRowRequest()
            column_to_get = ['gid', 'uid', 'index']

            primary_keys = []
            primary_keys.append([('gid',0), ('uid',0)])
            primary_keys.append([('gid',0), ('uid',1)])
            primary_keys.append([('gid',0), ('uid',2)])
            request.add(TableInBatchGetRowItem('myTable0', primary_keys, column_to_get, cond))

            primary_keys = []
            primary_keys.append([('gid',0), ('uid',0)])
            primary_keys.append([('gid',1), ('uid',0)])
            primary_keys.append([('gid',2), ('uid',0)])
            request.add(TableInBatchGetRowItem('myTable1', primary_keys, column_to_get, cond))

            result = client.batch_get_row(request)

            table0 = result.get_result_by_table('myTable0')
            table1 = result.get_result_by_table('myTable1')
        """
        response = self._request_helper('BatchGetRow', request)
        return BatchGetRowResponse(response)

    def batch_write_row(self, request):
        """
        Description: Batch modification of multiple rows.
        request = MiltiTableInBatchWriteRowItem()

        request.add(TableInBatchWriteRowItem(table0, row_items))
        request.add(TableInBatchWriteRowItem(table1, row_items))

        response = client.batch_write_row(request)

        ``response`` is the returned result, of type tablestore.metadata.BatchWriteRowResponse

        Example:
            # put
            row_items = []
            row = Row([('gid',0), ('uid', 0)], [('index', 6), ('addr', 'china')])
            row_items.append(PutRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

            # update
            row = Row([('gid',1), ('uid', 0)], {'put': [('index',9), ('addr', 'china')]})
            row_items.append(UpdateRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

            # delete
            row = Row([('gid', 2), ('uid', 0)])
            row_items.append(DeleteRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 3, ComparatorType.EQUAL, False)))

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem('myTable0', row_items))
            request.add(TableInBatchWriteRowItem('myTable1', row_items))

            result = self.client_test.batch_write_row(request)

            r0 = result.get_put_by_table('myTable0')
            r1 = result.get_put_by_table('myTable1')

        """

        response = self._request_helper('BatchWriteRow', request)

        return BatchWriteRowResponse(request, response)

    def get_range(self, table_name, direction,
                  inclusive_start_primary_key,
                  exclusive_end_primary_key,
                  columns_to_get=None,
                  limit=None,
                  column_filter=None,
                  max_version=1,
                  time_range=None,
                  start_column=None,
                  end_column=None,
                  token=None,
                  transaction_id=None):
        """
        Description: Retrieve multiple rows of data based on range conditions.

        ``table_name`` is the corresponding table name.
        ``direction`` indicates the direction of the range, in string format, with values including 'FORWARD' and 'BACKWARD'.
        ``inclusive_start_primary_key`` represents the starting primary key of the range (within the range).
        ``exclusive_end_primary_key`` represents the ending primary key of the range (not within the range).
        ``columns_to_get`` is an optional parameter that specifies a list of column names to retrieve; if not provided, all columns are retrieved.
        ``limit`` is an optional parameter that specifies the maximum number of rows to read; if not provided, there is no limit.
        ``column_filter`` is an optional parameter that specifies a condition for filtering rows.
        ``max_version`` is an optional parameter that specifies the maximum number of versions to return; either this or time_range must be specified.
        ``time_range`` is an optional parameter that specifies the range of versions to return; either this or max_version must be specified.
        ``start_column`` is an optional parameter used for wide row reading, indicating the starting column for this read operation.
        ``end_column`` is an optional parameter used for wide row reading, indicating the ending column for this read operation.
        ``token`` is an optional parameter used for wide row reading, indicating the starting column position for this read operation. It is binary-encoded and originates from the result of the previous request.

        Returns: A list of results that meet the specified conditions.

        ``consumed`` indicates the CapacityUnit consumed by this operation, which is an instance of the tablestore.metadata.CapacityUnit class.
        ``next_start_primary_key`` indicates the primary key column for the starting point of the next get_range operation, and its type is dict.
        ``row_list`` indicates the list of row data returned by this operation, formatted as [Row, ...].
        ``next_token`` indicates whether there are remaining attributes in the last row that have not been read. If next_token is not None, it means there are more to read, and this value should be filled in the next get_range call.

        Example:

            inclusive_start_primary_key = [('gid',1), ('uid',INF_MIN)]
            exclusive_end_primary_key = [('gid',4), ('uid',INF_MAX)]
            columns_to_get = ['name', 'address', 'mobile', 'age']
            consumed, next_start_primary_key, row_list, next_token = client.get_range(
                        'myTable', 'FORWARD',
                        inclusive_start_primary_key, exclusive_end_primary_key,
                        columns_to_get, 100
            )
        """

        return self._request_helper(
            'GetRange', table_name, direction,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get, limit,
            column_filter, max_version,
            time_range, start_column,
            end_column, token,
            transaction_id
        )

    def xget_range(self, table_name, direction,
                   inclusive_start_primary_key,
                   exclusive_end_primary_key,
                   consumed_counter,
                   columns_to_get=None,
                   count=None,
                   column_filter=None,
                   max_version=1,
                   time_range=None,
                   start_column=None,
                   end_column=None,
                   token=None):
        """
        Description: Retrieve multiple rows of data based on range conditions, iterator version.

        ``table_name`` is the corresponding table name.
        ``direction`` indicates the direction of the range, taking values FORWARD and BACKWARD from Direction.
        ``inclusive_start_primary_key`` represents the starting primary key of the range (within the range).
        ``exclusive_end_primary_key`` represents the ending primary key of the range (outside the range).
        ``consumed_counter`` is used for CapacityUnit consumption statistics and is an instance of the tablestore.metadata.CapacityUnit class.
        ``columns_to_get`` is an optional parameter, representing a list of column names to retrieve, with type list; if not specified, it retrieves all columns.
        ``count`` is an optional parameter, indicating the maximum number of rows to read; if not specified, it attempts to read all rows within the entire range.
        ``column_filter`` is an optional parameter, indicating the condition for reading specified rows.
        ``max_version`` is an optional parameter, indicating the maximum number of versions to return; either this or time_range must be specified.
        ``time_range`` is an optional parameter, indicating the range of versions to return; either this or max_version must be specified.
        ``start_column`` is an optional parameter, used for wide row reading, indicating the starting column for this read.
        ``end_column`` is an optional parameter, used for wide row reading, indicating the ending column for this read.
        ``token`` is an optional parameter, used for wide row reading, indicating the starting column position for this read. The content is encoded in binary and originates from the result of the previous request.

        Return: A list of results that meet the conditions.

        ``range_iterator`` is used to obtain an iterator for rows that meet the range conditions. Each element fetched has the format:
        row. Where row.primary_key represents the primary key columns, of list type,
        row.attribute_columns represent the attribute columns, also of list type. For other usages, see the iter type description.

        Example:

            consumed_counter = CapacityUnit(0, 0)
            inclusive_start_primary_key = [('gid',1), ('uid',INF_MIN)]
            exclusive_end_primary_key = [('gid',4), ('uid',INF_MAX)]
            columns_to_get = ['name', 'address', 'mobile', 'age']
            range_iterator = client.xget_range(
                        'myTable', Direction.FORWARD,
                        inclusive_start_primary_key, exclusive_end_primary_key,
                        consumed_counter, columns_to_get, 100
            )
            for row in range_iterator:
               pass
        """

        if not isinstance(consumed_counter, CapacityUnit):
            raise OTSClientError(
                "consumed_counter should be an instance of CapacityUnit, not %s" % (
                    consumed_counter.__class__.__name__)
            )
        left_count = None
        if count is not None:
            if count <= 0:
                raise OTSClientError("the value of count must be larger than 0")
            left_count = count

        consumed_counter.read = 0
        consumed_counter.write = 0
        next_start_pk = inclusive_start_primary_key
        while next_start_pk:
            consumed, next_start_pk, row_list, next_token = self.get_range(
                table_name, direction,
                next_start_pk, exclusive_end_primary_key,
                columns_to_get, left_count, column_filter,
                max_version, time_range, start_column,
                end_column, token
            )
            consumed_counter.read += consumed.read
            for row in row_list:
                yield row
                if left_count is not None:
                    left_count -= 1
                    if left_count <= 0:
                        return

    def list_search_index(self, table_name=None):
        """
        List all search indexes, or indexes under one table.

        :type table_name: str
        :param table_name: The name of table.

        Example usage:
            search_index_list = client.list_search_inex()
        """

        return self._request_helper('ListSearchIndex', table_name)

    def delete_search_index(self, table_name, index_name):
        """
        Delete the search index.

        Example usage:
            client.delete_search_index('table1', 'index1')
        """
        self._request_helper('DeleteSearchIndex', table_name, index_name)

    def create_search_index(self, table_name, index_name, index_meta):
        """
        Create search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        :type index_meta: tablestore.metadata.SearchIndexMeta
        :param index_meta: The definition of index, includes fields' schema, index setting, index pre-sorting configuration and TTL.

        Example usage:
            field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
            field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
            field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
            field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
            nested_field = FieldSchema('n', FieldType.NESTED, sub_field_schemas=
                [
                    FieldSchema('nk', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
                    FieldSchema('nt', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD),
                    FieldSchema('ng', FieldType.GEOPOINT, index=True, store=True, enable_sort_and_agg=True)
                ])
           fields = [field_a, field_b, field_c, field_d, nested_field]

           index_meta = SearchIndexMeta(fields, index_setting=None, index_sort=None)
           client.create_search_index('table_1', 'index_1', index_meta)
        """

        self._request_helper('CreateSearchIndex', table_name, index_name, index_meta)

    def update_search_index(self, table_name, index_name, index_meta):
        """
        Update search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        :type index_meta: tablestore.metadata.SearchIndexMeta
        :param index_meta: The definition of index, includes fields' schema, index setting , index pre-sorting configuration and TTL, Only support TTL in update_search_index

        Example usage:
           index_meta = SearchIndexMeta(fields=None, index_setting=None, index_sort=None, time_to_live = 94608000)
           client.update_search_index('table_1', 'index_1', index_meta)
        """

        self._request_helper('UpdateSearchIndex', table_name, index_name, index_meta)

    def describe_search_index(self, table_name, index_name):
        """
        Describe search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        Example usage:
            index_meta = client.describe_search_index('t1', 'index_1')
        """

        return self._request_helper('DescribeSearchIndex', table_name, index_name)

    def search(self, table_name, index_name, search_query, columns_to_get=None, routing_keys=None, timeout_s=None):
        """
        Perform search query on search index.

        Description:
        :type table_name: str
        :param table_name: The name of the table.

        :type index_name: str
        :param index_name: The name of the index.

        :type search_query: tablestore.metadata.SearchQuery
        :param search_query: The query to perform.

        :type columns_to_get: tablestore.metadata.ColumnsToGet
        :param columns_to_get: Columns to return.

        :type routing_keys: list
        :param routing_keys: List of routing keys.

        :type timeout_s: int
        :param timeout_s: timeout for search request.

        Returns: The result set of the query.

        ``search_response`` represents the result set of the query, including results from search, aggregation (agg), and group_by. It is an instance of the tablestore.metadata.SearchResponse class.

        Example usage:
            query = TermQuery('k', 'key000')
            search_response = client.search(table_name, index_name,
                              SearchQuery(query, limit=100),
                              ColumnsToGet(return_type=ColumnReturnType.ALL)
            )
        """

        return self._request_helper('Search', table_name, index_name, search_query, columns_to_get, routing_keys, timeout_s)

    def compute_splits(self, table_name, index_name):
        """
        Compute splits on search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        Returns: The result of the split computation.

        ``compute_splits_response`` represents the result of the split computation, which is an instance of the tablestore.metadata.ComputeSplitsResponse class.

        Example usage:
            compute_splits_response = client.compute_splits(table_name, index_name)
            )
        """

        return self._request_helper('ComputeSplits', table_name, index_name)

    def parallel_scan(self, table_name, index_name, scan_query, session_id, columns_to_get=None, timeout_s=None):
        """
        Perform parallel scan on search index.

        :type table_name: str
        :param table_name: The name of the table.

        :type index_name: str
        :param index_name: The name of the index.

        :type scan_query: tablestore.metadata.ScanQuery
        :param scan_query: The query to perform.

        :type session_id: str
        :param session_id: The ID of the session obtained from compute_splits_request's response.

        :type columns_to_get: tablestore.metadata.ColumnsToGet
        :param columns_to_get: Columns to return, allowed values: RETURN_SPECIFIED/RETURN_NONE/RETURN_ALL_FROM_INDEX

        :type timeout_s: int
        :param timeout_s: timeout for parallel_scan request.

        Returns: Result set of parallel scanning.

        ``parallel_scan_response`` represents the result of parallel scanning and is an instance of the tablestore.metadata.ParallelScanResponse class.


        Example usage:
            query = TermQuery('k', 'key000')
            parallel_scan_response = client.parallel_scan(
                table_name, index_name,
                ScanQuery(query, token=token_str, current_parallel_id=0, max_parallel=3, limit=100),
                ColumnsToGet(return_type=ColumnReturnType.RETURN_ALL_FROM_INDEX)
            )
        """

        return self._request_helper('ParallelScan', table_name, index_name, scan_query,
                                    session_id, columns_to_get, timeout_s)

    def create_secondary_index(self, table_name, index_meta, include_base_data):
        """
        Create a new secondary index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_meta: tablestore.metadata.SecondaryIndexMeta
        :param index_meta: The definition of index.

        :type include_base_data: bool
        :param include_base_data: Whether to include the data in the main table or not.

        Example usage:
            index_meta = SecondaryIndexMeta('index1', ['i', 's'], ['gid', 'uid', 'bool', 'b', 'd'])
            client.create_secondary_index(table_name, index_meta)
        """

        return self._request_helper('CreateIndex', table_name, index_meta, include_base_data)

    def delete_secondary_index(self, table_name, index_name):
        """
        Delete the secondary index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        Example usage:
            client.delete_secondary_index(table_name, index_name)
        """

        return self._request_helper('DropIndex', table_name, index_name)

    def start_local_transaction(self, table_name, key):
        """
        Start a local transaction and get the transaction id.

        :type table_name: str
        :param table_name: The name of the table.

        :type key: dict
        :param key: The partition key.

        Example usage:
            client.start_local_transaction(table_name, key)
        """

        return self._request_helper('StartLocalTransaction', table_name, key)

    def commit_transaction(self, transaction_id):
        """
        Commit a transaction by id.

        :type transaction_id: str
        :param transaction_id: The id of transaction.

        Example usage:
            client.commit_transaction(transaction_id)
        """

        return self._request_helper('CommitTransaction', transaction_id)

    def abort_transaction(self, transaction_id):
        """
        Abort a transaction by id.

        :type transaction_id: str
        :param transaction_id: The id of transaction.

        Example usage:
            client.abort_transaction(transaction_id)
        """

        return self._request_helper('AbortTransaction', transaction_id)

    def put_timeseries_data(self, timeseriesTableName: str, timeseriesRows: TimeseriesRow) -> PutTimeseriesDataResponse:

        return self._request_helper('PutTimeseriesData', timeseriesTableName, timeseriesRows)

    def create_timeseries_table(self, request: CreateTimeseriesTableRequest):
        return self._request_helper('CreateTimeseriesTable', request)

    def list_timeseries_table(self) -> list:
        return self._request_helper('ListTimeseriesTable')

    def delete_timeseries_table(self, timeseries_table_name: str):
        return self._request_helper('DeleteTimeseriesTable', timeseries_table_name)

    def describe_timeseries_table(self, timeseries_table_name: str) -> DescribeTimeseriesTableResponse:
        return self._request_helper('DescribeTimeseriesTable', timeseries_table_name)

    def update_timeseries_table(self, timeseries_meta: TimeseriesTableMeta):
        return self._request_helper('UpdateTimeseriesTable', timeseries_meta)

    def update_timeseries_meta(self, request: UpdateTimeseriesMetaRequest) -> UpdateTimeseriesMetaResponse:
        return self._request_helper('UpdateTimeseriesMeta', request)

    def delete_timeseries_meta(self, request: DeleteTimeseriesMetaRequest) -> DeleteTimeseriesMetaResponse:
        return self._request_helper('DeleteTimeseriesMeta', request)

    def query_timeseries_meta(self, request: QueryTimeseriesMetaRequest) -> QueryTimeseriesMetaResponse:
        return self._request_helper('QueryTimeseriesMeta', request)

    def get_timeseries_data(self, request: GetTimeseriesDataRequest) -> GetTimeseriesDataResponse:
        return self._request_helper('GetTimeseriesData', request)


class AsyncOTSClient(BaseOTSClient):

    DEFAULT_KEEPALIVE_TIMEOUT = 12

    def __init__(self, end_point, access_key_id=None, access_key_secret=None, instance_name=None,
                 credentials_provider: CredentialsProvider = None, region: str = None, **kwargs):
        """
        Initialize an ``AsyncOTSClient`` instance.

        Besides the init parameter of OTSClient, the following parameters are available or changed:

        ``keepalive_timeout`` timeout for connection reusing after releasing (optional). Values 0. For disabling keep-alive feature use force_close=True flag. The default value is 12 seconds.

        ``force_close`` close underlying sockets after connection releasing (optional). The default is False.

        ``socket_timeout`` is the Socket timeout for each connection in the connection pool, measured in seconds. It can be an [int|float|tuple|list].
                           If tuple or list, the format is (conn_timeout, read_timeout). The default value is 50.

        ``ssl_version`` defines the minimum TLS version used for https connections. The default is None.
        """
        super().__init__(
            end_point=end_point,
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            instance_name=instance_name,
            credentials_provider=credentials_provider,
            region=region,
            **kwargs,
        )

        self.keepalive_timeout = kwargs.get('keepalive_timeout', self.DEFAULT_KEEPALIVE_TIMEOUT)
        self.force_close = kwargs.get('force_close', False)
        self._connection = None

    def _get_or_create_connection(self):
        if self._connection is None:
            self._connection = AsyncConnectionPool(
                self.host, self.path,
                timeout=self.socket_timeout,
                maxsize=self.max_connection,
                keepalive_timeout=self.keepalive_timeout,
                force_close=self.force_close,
                client_ssl_version=self.ssl_version
            )
        return self._connection

    async def close(self):
        connection = self._connection # prevent concurrency issues in coroutines, if after close, set this value to None, other coroutine may get a closed connection
        self._connection = None
        await connection.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _request_helper(self, api_name, *args, **kwargs):
        # Generate signing key, each request generate once
        # Must generate before making request headers
        connection = self._get_or_create_connection()

        self._signer.gen_signing_key()
        query, req_headers, req_body = self.protocol.make_request(api_name, self._signer, *args, **kwargs)

        retry_times = 0
        while True:
            try:
                status, reason, res_headers, res_body = await connection.send_receive(query, req_headers, req_body)
                self.protocol.handle_error(api_name, query, status, reason, res_headers, res_body, self._signer)
                break
            except OTSServiceError as e:
                if self.retry_policy.should_retry(retry_times, e, api_name):
                    retry_delay = self.retry_policy.get_retry_delay(retry_times, e, api_name)
                    await asyncio.sleep(retry_delay)
                    retry_times += 1
                else:
                    raise e

        return self.protocol.parse_response(api_name, status, res_headers, res_body)

    async def create_table(self, table_meta, table_options, reserved_throughput, secondary_indexes=None):
        """
        Description: Creates a table based on the table information.

        ``table_meta`` is an instance of the ``tablestore.metadata.TableMeta`` class. It includes the table name and the schema of the PrimaryKey.
        Refer to the documentation for the ``TableMeta`` class. After creating a table, it usually takes about 1 minute for partition loading to complete before various operations can be performed.
        ``table_options`` is an instance of the ``tablestore.metadata.TableOptions`` class, which includes three parameters: time_to_live, max_version, and max_time_deviation.
        ``reserved_throughput`` is an instance of the ``tablestore.metadata.ReservedThroughput`` class, representing the reserved read/write throughput.
        ``secondary_indexes`` is an array that can include one or more instances of the ``tablestore.metadata.SecondaryIndexMeta`` class, representing the secondary indexes to be created.

        Return: None.

        Example:

            schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
            table_meta = TableMeta('myTable', schema_of_primary_key)
            table_options = TableOptions()
            reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
            await client.create_table(table_meta, table_options, reserved_throughput)
        """

        if secondary_indexes is None:
            secondary_indexes = []
        await self._request_helper('CreateTable', table_meta, table_options, reserved_throughput, secondary_indexes)

    async def delete_table(self, table_name):
        """
        Description: Delete a table according to the table name.

        ``table_name`` is the corresponding table name.

        Return: None.

        Example:

            await client.delete_table('myTable')
        """

        await self._request_helper('DeleteTable', table_name)

    async def list_table(self):
        """
        Description: Get a list of all table names.

        Return: A list of table names.

        ``table_list`` represents the list of table names obtained, which is of type tuple, e.g., ('MyTable1', 'MyTable2').

        Example:

            table_list = await client.list_table()
        """

        return await self._request_helper('ListTable')

    async def update_table(self, table_name, table_options=None, reserved_throughput=None):
        """
        Description: Update table properties, currently only supports modifying the reserved read/write throughput.

        ``table_name`` is the corresponding table name.
        ``table_options`` is an instance of the ``tablestore.metadata.TableOptions`` class, which includes three parameters: time_to_live, max_version, and max_time_deviation.
        ``reserved_throughput`` is an instance of the ``tablestore.metadata.ReservedThroughput`` class, representing the reserved read/write throughput.

        Return: The most recent increase time, decrease time, and the number of decreases on the same day for the reserved read/write throughput of this table.

        ``update_table_response`` represents the result of the update, which is an instance of the ``tablestore.metadata.UpdateTableResponse`` class.

        Example:

            reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
            table_options = TableOptions();
            update_response = await client.update_table('myTable', table_options, reserved_throughput)
        """

        return await self._request_helper(
            'UpdateTable', table_name, table_options, reserved_throughput
        )

    async def describe_table(self, table_name):
        """
        Description: Get the description information of the table.

        ``table_name`` is the corresponding table name.

        Return: The description information of the table.

        ``describe_table_response`` represents the description information of the table, which is an instance of the tablestore.metadata.DescribeTableResponse class.

        Example:

            describe_table_response = await client.describe_table('myTable')
        """

        return await self._request_helper('DescribeTable', table_name)

    async def get_row(self, table_name, primary_key, columns_to_get=None,
                column_filter=None, max_version=1, time_range=None,
                start_column=None, end_column=None, token=None,
                transaction_id=None):
        """
        Description: Get a single row of data.

        ``table_name`` is the corresponding table name.
        ``primary_key`` is the primary key, with a type of dict.
        ``columns_to_get`` is an optional parameter, representing a list of column names to retrieve, with a type of list; if not specified, it retrieves all columns.
        ``column_filter`` is an optional parameter, indicating a filter for reading rows based on specific conditions.
        ``max_version`` is an optional parameter, indicating the maximum number of versions to read.
        ``time_range`` is an optional parameter, indicating the version range or specific version to read, and at least one of ``time_range`` or ``max_version`` must be provided.

        Return: The consumed CapacityUnit for this operation, primary key columns, and attribute columns.

        ``consumed`` indicates the consumed CapacityUnit, which is an instance of the tablestore.metadata.CapacityUnit class.
        ``return_row`` indicates the row data, including primary key columns and attribute columns, both of type list, such as: [('PK0', value0), ('PK1', value1)].
        ``next_token`` indicates the position for the next read when reading wide rows, encoded as binary.

        Example:

            primary_key = [('gid', 1), ('uid', 101)]
            columns_to_get = ['name', 'address', 'age']
            consumed, return_row, next_token = await client.get_row('myTable', primary_key, columns_to_get)
        """

        return await self._request_helper(
            'GetRow', table_name, primary_key, columns_to_get,
            column_filter, max_version, time_range,
            start_column, end_column, token, transaction_id
        )

    async def put_row(self, table_name, row, condition=None, return_type=None, transaction_id=None):
        """
        Description: Write a row of data. Returns the CapacityUnit consumed by this operation.

        ``table_name`` is the corresponding table name.
        ``row`` is the row data, including the primary key and attribute columns.
        ``condition`` indicates a condition check to be performed before executing the operation; the operation will only execute if the condition is met. It is an instance of the tablestore.metadata.Condition class.
        Currently, two types of condition checks are supported: one is a check on the existence of the row, with possible conditions including 'IGNORE', 'EXPECT_EXIST', and 'EXPECT_NOT_EXIST'; the other is a condition check on the value of attribute columns.
        ``return_type`` indicates the return type, which is an instance of the tablestore.metadata.ReturnType class. Currently, it only supports returning the PrimaryKey, typically used in scenarios involving auto-increment of primary key columns.

        Return: The CapacityUnit consumed by this operation and the requested row data.

        ``consumed`` represents the consumed CapacityUnit, which is an instance of the tablestore.metadata.CapacityUnit class.
        ``return_row`` represents the returned row data, which may include the primary key and attribute columns.

        Example:

            primary_key = [('gid',1), ('uid',101)]
            attribute_columns = [('name','张三'), ('mobile',111111111), ('address','中国A地'), ('age',20)]
            row = Row(primary_key, attribute_columns)
            condition = Condition('EXPECT_NOT_EXIST')
            consumed, return_row = await client.put_row('myTable', row, condition)
        """

        return await self._request_helper(
            'PutRow', table_name, row, condition, return_type, transaction_id
        )

    async def update_row(self, table_name, row, condition, return_type=None, transaction_id=None):
        """
        Description: Update a row of data.

        ``table_name`` is the corresponding table name.
        ``row`` represents the updated row data, including primary key columns and attribute columns. Primary key columns are lists; attribute columns are dictionaries.
        ``condition`` indicates performing a condition check before executing the operation, and the operation will only be executed if the condition is met. It is an instance of the tablestore.metadata.Condition class.
        Currently, two types of condition checks are supported: one is checking the existence of the row, with check conditions including 'IGNORE', 'EXPECT_EXIST', and 'EXPECT_NOT_EXIST'; the other is condition checks on the values of attribute columns.
        ``return_type`` represents the return type, which is an instance of the tablestore.metadata.ReturnType class. Currently, it only supports returning the PrimaryKey, generally used for auto-increment in primary key columns.

        Return: The CapacityUnit consumed by this operation and the row data to be returned (return_row).

        consumed represents the CapacityUnit consumed, which is an instance of the tablestore.metadata.CapacityUnit class.
        return_row represents the row data to be returned.

        Example:

            primary_key = [('gid',1), ('uid',101)]
            update_of_attribute_columns = {
                'put' : [('name','Zhang Sanfeng'), ('address','Location B, China')],
                'delete' : [('mobile', 1493725896147)],
                'delete_all' : [('age')],
                'increment' : [('counter', 1)]
            }
            row = Row(primary_key, update_of_attribute_columns)
            condition = Condition('EXPECT_EXIST')
            consumed = await client.update_row('myTable', row, condition)
        """

        return await self._request_helper(
            'UpdateRow', table_name, row, condition, return_type, transaction_id
        )

    async def delete_row(self, table_name, row=None, condition=None, return_type=None, transaction_id=None, **kwargs):
        """
        Description: Delete a row of data.

        ``table_name`` is the corresponding table name.
        ``row`` represents the primary key.
        ``condition`` indicates a condition check performed before the operation, which executes only if the condition is met. It is an instance of the tablestore.metadata.Condition class.
        Currently, two types of condition checks are supported: one checks the existence of the row, with possible conditions including 'IGNORE', 'EXPECT_EXIST', and 'EXPECT_NOT_EXIST'; the other performs a condition check on the value of attribute columns.

        Return: The CapacityUnit consumed by this operation and the row data to be returned (`return_row`).

        `consumed` indicates the consumed CapacityUnit, which is an instance of the tablestore.metadata.CapacityUnit class.
        `return_row` indicates the row data to be returned.

        Example:

            primary_key = [('gid',1), ('uid',101)]
            row = Row(primary_key)
            condition = Condition('IGNORE')
            consumed, return_row = await client.delete_row('myTable', row, condition)
        """

        primary_key = kwargs.get('primary_key', None)
        # When row is not empty, the row parameter will be used preferentially, but the primary_key parameter is required for passing parameters.
        if row is not None:
            primary_key = row
        # When passing Row as a parameter, extract the primary_key
        if isinstance(primary_key, Row):
            primary_key = primary_key.primary_key
        return await self._request_helper(
            'DeleteRow', table_name, primary_key, condition, return_type, transaction_id
        )

    async def exe_sql_query(self, query):
        """
        Description: Executes an SQL query.

        ``query`` is the query to be executed.

        (rows, table_capacity_units, search_capacity_units)

        Returns:
        ``table_capacity_units``  The CapacityUnit consumed for each table by this operation
        ``search_capacity_units`` The CapacityUnit consumed for each search by this operation
        ``rows``                  The returned data

        Example:
        row_list, table_consume_list, search_consume_list = await client.exe_sql_query(query)
        """
        return await self._request_helper(
            'SQLQuery', query
        )

    async def batch_get_row(self, request):
        """
        Description: Batch retrieve multiple rows of data.
        request = BatchGetRowRequest()

        request.add(TableInBatchGetRowItem(myTable0, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable1, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable2, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable3, primary_keys, column_to_get=None, column_filter=None))

        response = await client.batch_get_row(request)

        ``response`` is the returned result, of type tablestore.metadata.BatchGetRowResponse

        Example:
            cond = CompositeColumnCondition(LogicalOperator.AND)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

            request = BatchGetRowRequest()
            column_to_get = ['gid', 'uid', 'index']

            primary_keys = []
            primary_keys.append([('gid',0), ('uid',0)])
            primary_keys.append([('gid',0), ('uid',1)])
            primary_keys.append([('gid',0), ('uid',2)])
            request.add(TableInBatchGetRowItem('myTable0', primary_keys, column_to_get, cond))

            primary_keys = []
            primary_keys.append([('gid',0), ('uid',0)])
            primary_keys.append([('gid',1), ('uid',0)])
            primary_keys.append([('gid',2), ('uid',0)])
            request.add(TableInBatchGetRowItem('myTable1', primary_keys, column_to_get, cond))

            result = await client.batch_get_row(request)

            table0 = result.get_result_by_table('myTable0')
            table1 = result.get_result_by_table('myTable1')
        """
        response = await self._request_helper('BatchGetRow', request)
        return BatchGetRowResponse(response)

    async def batch_write_row(self, request):
        """
        Description: Batch modification of multiple rows.
        request = MiltiTableInBatchWriteRowItem()

        request.add(TableInBatchWriteRowItem(table0, row_items))
        request.add(TableInBatchWriteRowItem(table1, row_items))

        response = await client.batch_write_row(request)

        ``response`` is the returned result, of type tablestore.metadata.BatchWriteRowResponse

        Example:
            # put
            row_items = []
            row = Row([('gid',0), ('uid', 0)], [('index', 6), ('addr', 'china')])
            row_items.append(PutRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

            # update
            row = Row([('gid',1), ('uid', 0)], {'put': [('index',9), ('addr', 'china')]})
            row_items.append(UpdateRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

            # delete
            row = Row([('gid', 2), ('uid', 0)])
            row_items.append(DeleteRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 3, ComparatorType.EQUAL, False)))

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem('myTable0', row_items))
            request.add(TableInBatchWriteRowItem('myTable1', row_items))

            result = await self.client_test.batch_write_row(request)

            r0 = result.get_put_by_table('myTable0')
            r1 = result.get_put_by_table('myTable1')

        """

        response = await self._request_helper('BatchWriteRow', request)

        return BatchWriteRowResponse(request, response)

    async def get_range(self, table_name, direction,
                  inclusive_start_primary_key,
                  exclusive_end_primary_key,
                  columns_to_get=None,
                  limit=None,
                  column_filter=None,
                  max_version=1,
                  time_range=None,
                  start_column=None,
                  end_column=None,
                  token=None,
                  transaction_id=None):
        """
        Description: Retrieve multiple rows of data based on range conditions.

        ``table_name`` is the corresponding table name.
        ``direction`` indicates the direction of the range, in string format, with values including 'FORWARD' and 'BACKWARD'.
        ``inclusive_start_primary_key`` represents the starting primary key of the range (within the range).
        ``exclusive_end_primary_key`` represents the ending primary key of the range (not within the range).
        ``columns_to_get`` is an optional parameter that specifies a list of column names to retrieve; if not provided, all columns are retrieved.
        ``limit`` is an optional parameter that specifies the maximum number of rows to read; if not provided, there is no limit.
        ``column_filter`` is an optional parameter that specifies a condition for filtering rows.
        ``max_version`` is an optional parameter that specifies the maximum number of versions to return; either this or time_range must be specified.
        ``time_range`` is an optional parameter that specifies the range of versions to return; either this or max_version must be specified.
        ``start_column`` is an optional parameter used for wide row reading, indicating the starting column for this read operation.
        ``end_column`` is an optional parameter used for wide row reading, indicating the ending column for this read operation.
        ``token`` is an optional parameter used for wide row reading, indicating the starting column position for this read operation. It is binary-encoded and originates from the result of the previous request.

        Returns: A list of results that meet the specified conditions.

        ``consumed`` indicates the CapacityUnit consumed by this operation, which is an instance of the tablestore.metadata.CapacityUnit class.
        ``next_start_primary_key`` indicates the primary key column for the starting point of the next get_range operation, and its type is dict.
        ``row_list`` indicates the list of row data returned by this operation, formatted as [Row, ...].
        ``next_token`` indicates whether there are remaining attributes in the last row that have not been read. If next_token is not None, it means there are more to read, and this value should be filled in the next get_range call.

        Example:

            inclusive_start_primary_key = [('gid',1), ('uid',INF_MIN)]
            exclusive_end_primary_key = [('gid',4), ('uid',INF_MAX)]
            columns_to_get = ['name', 'address', 'mobile', 'age']
            consumed, next_start_primary_key, row_list, next_token = await client.get_range(
                        'myTable', 'FORWARD',
                        inclusive_start_primary_key, exclusive_end_primary_key,
                        columns_to_get, 100
            )
        """

        return await self._request_helper(
            'GetRange', table_name, direction,
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get, limit,
            column_filter, max_version,
            time_range, start_column,
            end_column, token,
            transaction_id
        )

    async def xget_range(self, table_name, direction,
                   inclusive_start_primary_key,
                   exclusive_end_primary_key,
                   consumed_counter,
                   columns_to_get=None,
                   count=None,
                   column_filter=None,
                   max_version=1,
                   time_range=None,
                   start_column=None,
                   end_column=None,
                   token=None):
        """
        Description: Retrieve multiple rows of data based on range conditions, iterator version.

        ``table_name`` is the corresponding table name.
        ``direction`` indicates the direction of the range, taking values FORWARD and BACKWARD from Direction.
        ``inclusive_start_primary_key`` represents the starting primary key of the range (within the range).
        ``exclusive_end_primary_key`` represents the ending primary key of the range (outside the range).
        ``consumed_counter`` is used for CapacityUnit consumption statistics and is an instance of the tablestore.metadata.CapacityUnit class.
        ``columns_to_get`` is an optional parameter, representing a list of column names to retrieve, with type list; if not specified, it retrieves all columns.
        ``count`` is an optional parameter, indicating the maximum number of rows to read; if not specified, it attempts to read all rows within the entire range.
        ``column_filter`` is an optional parameter, indicating the condition for reading specified rows.
        ``max_version`` is an optional parameter, indicating the maximum number of versions to return; either this or time_range must be specified.
        ``time_range`` is an optional parameter, indicating the range of versions to return; either this or max_version must be specified.
        ``start_column`` is an optional parameter, used for wide row reading, indicating the starting column for this read.
        ``end_column`` is an optional parameter, used for wide row reading, indicating the ending column for this read.
        ``token`` is an optional parameter, used for wide row reading, indicating the starting column position for this read. The content is encoded in binary and originates from the result of the previous request.

        Return: A list of results that meet the conditions.

        ``range_iterator`` is used to obtain an iterator for rows that meet the range conditions. Each element fetched has the format:
        row. Where row.primary_key represents the primary key columns, of list type,
        row.attribute_columns represent the attribute columns, also of list type. For other usages, see the iter type description.

        Example:

            consumed_counter = CapacityUnit(0, 0)
            inclusive_start_primary_key = [('gid',1), ('uid',INF_MIN)]
            exclusive_end_primary_key = [('gid',4), ('uid',INF_MAX)]
            columns_to_get = ['name', 'address', 'mobile', 'age']
            range_iterator = client.xget_range(
                        'myTable', Direction.FORWARD,
                        inclusive_start_primary_key, exclusive_end_primary_key,
                        consumed_counter, columns_to_get, 100
            )
            async for row in range_iterator:
               pass
        """

        if not isinstance(consumed_counter, CapacityUnit):
            raise OTSClientError(
                "consumed_counter should be an instance of CapacityUnit, not %s" % (
                    consumed_counter.__class__.__name__)
            )
        left_count = None
        if count is not None:
            if count <= 0:
                raise OTSClientError("the value of count must be larger than 0")
            left_count = count

        consumed_counter.read = 0
        consumed_counter.write = 0
        next_start_pk = inclusive_start_primary_key
        while next_start_pk:
            consumed, next_start_pk, row_list, next_token = await self.get_range(
                table_name, direction,
                next_start_pk, exclusive_end_primary_key,
                columns_to_get, left_count, column_filter,
                max_version, time_range, start_column,
                end_column, token
            )
            consumed_counter.read += consumed.read
            for row in row_list:
                yield row
                if left_count is not None:
                    left_count -= 1
                    if left_count <= 0:
                        return

    async def list_search_index(self, table_name=None):
        """
        List all search indexes, or indexes under one table.

        :type table_name: str
        :param table_name: The name of table.

        Example usage:
            search_index_list = await client.list_search_inex()
        """

        return await self._request_helper('ListSearchIndex', table_name)

    async def delete_search_index(self, table_name, index_name):
        """
        Delete the search index.

        Example usage:
            await client.delete_search_index('table1', 'index1')
        """
        await self._request_helper('DeleteSearchIndex', table_name, index_name)

    async def create_search_index(self, table_name, index_name, index_meta):
        """
        Create search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        :type index_meta: tablestore.metadata.SearchIndexMeta
        :param index_meta: The definition of index, includes fields' schema, index setting, index pre-sorting configuration and TTL.

        Example usage:
            field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
            field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
            field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
            field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
            nested_field = FieldSchema('n', FieldType.NESTED, sub_field_schemas=
                [
                    FieldSchema('nk', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
                    FieldSchema('nt', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD),
                    FieldSchema('ng', FieldType.GEOPOINT, index=True, store=True, enable_sort_and_agg=True)
                ])
           fields = [field_a, field_b, field_c, field_d, nested_field]

           index_meta = SearchIndexMeta(fields, index_setting=None, index_sort=None)
           await client.create_search_index('table_1', 'index_1', index_meta)
        """

        await self._request_helper('CreateSearchIndex', table_name, index_name, index_meta)

    async def update_search_index(self, table_name, index_name, index_meta):
        """
        Update search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        :type index_meta: tablestore.metadata.SearchIndexMeta
        :param index_meta: The definition of index, includes fields' schema, index setting , index pre-sorting configuration and TTL, Only support TTL in update_search_index

        Example usage:
           index_meta = SearchIndexMeta(fields=None, index_setting=None, index_sort=None, time_to_live = 94608000)
           await client.update_search_index('table_1', 'index_1', index_meta)
        """

        await self._request_helper('UpdateSearchIndex', table_name, index_name, index_meta)

    async def describe_search_index(self, table_name, index_name):
        """
        Describe search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        Example usage:
            index_meta = await client.describe_search_index('t1', 'index_1')
        """

        return await self._request_helper('DescribeSearchIndex', table_name, index_name)

    async def search(self, table_name, index_name, search_query, columns_to_get=None, routing_keys=None, timeout_s=None):
        """
        Perform search query on search index.

        Description:
        :type table_name: str
        :param table_name: The name of the table.

        :type index_name: str
        :param index_name: The name of the index.

        :type search_query: tablestore.metadata.SearchQuery
        :param search_query: The query to perform.

        :type columns_to_get: tablestore.metadata.ColumnsToGet
        :param columns_to_get: Columns to return.

        :type routing_keys: list
        :param routing_keys: List of routing keys.

        :type timeout_s: int
        :param timeout_s: timeout for search request.

        Returns: The result set of the query.

        ``search_response`` represents the result set of the query, including results from search, aggregation (agg), and group_by. It is an instance of the tablestore.metadata.SearchResponse class.

        Example usage:
            query = TermQuery('k', 'key000')
            search_response = await client.search(
                table_name, index_name,
                SearchQuery(query, limit=100),
                ColumnsToGet(return_type=ColumnReturnType.ALL)
            )
        """

        return await self._request_helper('Search', table_name, index_name, search_query, columns_to_get, routing_keys,
                                    timeout_s)

    async def compute_splits(self, table_name, index_name):
        """
        Compute splits on search index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        Returns: The result of the split computation.

        ``compute_splits_response`` represents the result of the split computation, which is an instance of the tablestore.metadata.ComputeSplitsResponse class.

        Example usage:
            compute_splits_response = await client.compute_splits(table_name, index_name)
        """

        return await self._request_helper('ComputeSplits', table_name, index_name)

    async def parallel_scan(self, table_name, index_name, scan_query, session_id, columns_to_get=None, timeout_s=None):
        """
        Perform parallel scan on search index.

        :type table_name: str
        :param table_name: The name of the table.

        :type index_name: str
        :param index_name: The name of the index.

        :type scan_query: tablestore.metadata.ScanQuery
        :param scan_query: The query to perform.

        :type session_id: str
        :param session_id: The ID of the session obtained from compute_splits_request's response.

        :type columns_to_get: tablestore.metadata.ColumnsToGet
        :param columns_to_get: Columns to return, allowed values: RETURN_SPECIFIED/RETURN_NONE/RETURN_ALL_FROM_INDEX

        :type timeout_s: int
        :param timeout_s: timeout for parallel_scan request.

        Returns: Result set of parallel scanning.

        ``parallel_scan_response`` represents the result of parallel scanning and is an instance of the tablestore.metadata.ParallelScanResponse class.


        Example usage:
            query = TermQuery('k', 'key000')
            parallel_scan_response = await client.parallel_scan(
                table_name, index_name,
                ScanQuery(query, token=token_str, current_parallel_id=0, max_parallel=3, limit=100),
                ColumnsToGet(return_type=ColumnReturnType.RETURN_ALL_FROM_INDEX)
            )
        """

        return await self._request_helper('ParallelScan', table_name, index_name, scan_query,
                                    session_id, columns_to_get, timeout_s)

    async def create_secondary_index(self, table_name, index_meta, include_base_data):
        """
        Create a new secondary index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_meta: tablestore.metadata.SecondaryIndexMeta
        :param index_meta: The definition of index.

        :type include_base_data: bool
        :param include_base_data: Whether to include the data in the main table or not.

        Example usage:
            index_meta = SecondaryIndexMeta('index1', ['i', 's'], ['gid', 'uid', 'bool', 'b', 'd'])
            await client.create_secondary_index(table_name, index_meta)
        """

        return await self._request_helper('CreateIndex', table_name, index_meta, include_base_data)

    async def delete_secondary_index(self, table_name, index_name):
        """
        Delete the secondary index.

        :type table_name: str
        :param table_name: The name of table.

        :type index_name: str
        :param index_name: The name of index.

        Example usage:
            await client.delete_secondary_index(table_name, index_name)
        """

        return await self._request_helper('DropIndex', table_name, index_name)

    async def start_local_transaction(self, table_name, key):
        """
        Start a local transaction and get the transaction id.

        :type table_name: str
        :param table_name: The name of the table.

        :type key: dict
        :param key: The partition key.

        Example usage:
            await client.start_local_transaction(table_name, key)
        """

        return await self._request_helper('StartLocalTransaction', table_name, key)

    async def commit_transaction(self, transaction_id):
        """
        Commit a transaction by id.

        :type transaction_id: str
        :param transaction_id: The id of transaction.

        Example usage:
            await client.commit_transaction(transaction_id)
        """

        return await self._request_helper('CommitTransaction', transaction_id)

    async def abort_transaction(self, transaction_id):
        """
        Abort a transaction by id.

        :type transaction_id: str
        :param transaction_id: The id of transaction.

        Example usage:
            await client.abort_transaction(transaction_id)
        """

        return await self._request_helper('AbortTransaction', transaction_id)

    async def put_timeseries_data(self, timeseriesTableName: str, timeseriesRows: TimeseriesRow) -> PutTimeseriesDataResponse:

        return await self._request_helper('PutTimeseriesData', timeseriesTableName, timeseriesRows)

    async def create_timeseries_table(self, request: CreateTimeseriesTableRequest):
        return await self._request_helper('CreateTimeseriesTable', request)

    async def list_timeseries_table(self) -> list:
        return await self._request_helper('ListTimeseriesTable')

    async def delete_timeseries_table(self, timeseries_table_name: str):
        return await self._request_helper('DeleteTimeseriesTable', timeseries_table_name)

    async def describe_timeseries_table(self, timeseries_table_name: str) -> DescribeTimeseriesTableResponse:
        return await self._request_helper('DescribeTimeseriesTable', timeseries_table_name)

    async def update_timeseries_table(self, timeseries_meta: TimeseriesTableMeta):
        return await self._request_helper('UpdateTimeseriesTable', timeseries_meta)

    async def update_timeseries_meta(self, request: UpdateTimeseriesMetaRequest) -> UpdateTimeseriesMetaResponse:
        return await self._request_helper('UpdateTimeseriesMeta', request)

    async def delete_timeseries_meta(self, request: DeleteTimeseriesMetaRequest) -> DeleteTimeseriesMetaResponse:
        return await self._request_helper('DeleteTimeseriesMeta', request)

    async def query_timeseries_meta(self, request: QueryTimeseriesMetaRequest) -> QueryTimeseriesMetaResponse:
        return await self._request_helper('QueryTimeseriesMeta', request)

    async def get_timeseries_data(self, request: GetTimeseriesDataRequest) -> GetTimeseriesDataResponse:
        return await self._request_helper('GetTimeseriesData', request)