# -*- coding: utf8 -*-
import asyncio

from aiohttp import ClientTimeout

from tablestore import *
import unittest
from unittest import IsolatedAsyncioTestCase
from .lib import test_config


class AsyncTest(IsolatedAsyncioTestCase):
    def setUp(self):
        self.table_name = 'AsyncTest'
        self.rows_count = 100

        self.async_client = AsyncOTSClient(test_config.OTS_ENDPOINT, test_config.OTS_ACCESS_KEY_ID, test_config.OTS_ACCESS_KEY_SECRET, test_config.OTS_INSTANCE, region=test_config.OTS_REGION)

        asyncio.run(self._prepare_empty_table())

    def _get_batch_write_request(self, id_base = 0):
        put_row_items = []

        for i in range(id_base, id_base + self.rows_count):
            primary_key = [('gid', i), ('uid', i + 1)]
            attribute_columns = [('name', 'somebody' + str(i)), ('age', i)]
            row = Row(primary_key, attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            item = PutRowItem(row, condition)
            put_row_items.append(item)

        batch_write_request = BatchWriteRowRequest()
        batch_write_request.add(TableInBatchWriteRowItem(self.table_name, put_row_items))

        return batch_write_request

    def _get_batch_get_request(self, id_base = 0):
        columns_to_get = ['name', 'age']
        rows_to_get = []
        for i in range(id_base, id_base + self.rows_count):
            primary_key = [('gid', i), ('uid', i + 1)]
            rows_to_get.append(primary_key)

        batch_get_request = BatchGetRowRequest()
        batch_get_request.add(TableInBatchGetRowItem(self.table_name, rows_to_get, columns_to_get, max_version=1))

        return batch_get_request

    async def _prepare_empty_table(self):
        if self.table_name in (await self.async_client.list_table()):
            await self.async_client.delete_table(self.table_name)

        schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
        table_meta = TableMeta(self.table_name, schema_of_primary_key)
        table_options = TableOptions()
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        await self.async_client.create_table(table_meta, table_options, reserved_throughput)

        await self.async_client.close()

        await asyncio.sleep(0.02)

    def _get_result_rows(self, result):
        return len([item for item in result.items[self.table_name] if item.row is not None])

    async def test_batch_write(self):
        async with self.async_client:
            await self.async_client.batch_write_row(self._get_batch_write_request())
            result = await self.async_client.batch_get_row(self._get_batch_get_request(id_base=0))
            self.assertEqual(self._get_result_rows(result), self.rows_count)

        await self._prepare_empty_table()

    async def test_parallel(self):
        paragraph = 3

        async with self.async_client:
            tasks = [
                asyncio.create_task(self.async_client.batch_write_row(self._get_batch_write_request(id_base=i * self.rows_count)))
                for i in range(paragraph)
            ]
            for task in tasks:
                await task

            tasks = [
                self.async_client.batch_get_row(self._get_batch_get_request(id_base=i * self.rows_count))
                for i in range(paragraph)
            ]
            results = await asyncio.gather(*tasks)

            for result in results:
                self.assertEqual(self._get_result_rows(result), self.rows_count)

        await self._prepare_empty_table()


    async def test_double_close(self):
        async with self.async_client:
            await self.async_client.batch_write_row(self._get_batch_write_request(id_base=0))

        async with self.async_client:
            result = await self.async_client.batch_get_row(self._get_batch_get_request(id_base=0))
            self.assertEqual(self._get_result_rows(result), self.rows_count)

        await self.async_client.batch_write_row(self._get_batch_write_request(id_base=0))
        await self.async_client.close()

        result = await self.async_client.batch_get_row(self._get_batch_get_request(id_base=0))
        self.assertEqual(self._get_result_rows(result), self.rows_count)
        await self.async_client.close()

        await self._prepare_empty_table()

    async def test_timeout_parameter(self):
        async with AsyncOTSClient(
                test_config.OTS_ENDPOINT,
                test_config.OTS_ACCESS_KEY_ID,
                test_config.OTS_ACCESS_KEY_SECRET,
                test_config.OTS_INSTANCE,
                region=test_config.OTS_REGION,
                socket_timeout=10
        ) as client:
            await client.batch_get_row(self._get_batch_get_request(id_base=0))
            self.assertEqual(client._connection.pool._timeout, ClientTimeout(sock_connect=10, sock_read=10))

        async with AsyncOTSClient(
                test_config.OTS_ENDPOINT,
                test_config.OTS_ACCESS_KEY_ID,
                test_config.OTS_ACCESS_KEY_SECRET,
                test_config.OTS_INSTANCE,
                region=test_config.OTS_REGION,
                socket_timeout=(10, 30)
        ) as client:
            await client.batch_get_row(self._get_batch_get_request(id_base=0))
            self.assertEqual(client._connection.pool._timeout, ClientTimeout(sock_connect=10, sock_read=30))

if __name__ == '__main__':
    unittest.main()