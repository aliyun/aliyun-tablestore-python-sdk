import asyncio
import logging
import random
from collections.abc import AsyncGenerator

import tablestore
from tablestore import OTSClient, AsyncOTSClient, CredentialsProvider


class RandomOTSClient:
    def __init__(self, end_point, access_key_id=None, access_key_secret=None, instance_name=None,
                 credentials_provider: CredentialsProvider = None, region: str = None, **kwargs):
        self.sync_client = OTSClient(end_point, access_key_id, access_key_secret, instance_name, credentials_provider, region, **kwargs)
        self.async_client = AsyncOTSClient(end_point, access_key_id, access_key_secret, instance_name, credentials_provider, region, **kwargs)

        self.logger = None
        self.set_logger()

        func_names = [
            func_name for func_name in dir(OTSClient)
            if callable(getattr(OTSClient, func_name)) and not func_name.startswith('_')
        ]
        for func_name in func_names:
            setattr(self, func_name, self.generate_func_by_name(func_name))

    def set_logger(self):
        self.logger = logging.getLogger('APITestBase')
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler('tablestore_sdk_test.log')
        fh.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    async def _run_async_client_func(self, func_name, *args, **kwargs):
        async with self.async_client:
            coroutine_object = getattr(self.async_client, func_name)(*args, **kwargs)
            if isinstance(coroutine_object, AsyncGenerator):
                result = [item async for item in coroutine_object]
            else:
                result = await coroutine_object

        return result

    def generate_func_by_name(self, func_name):
        def generated_func(*args, **kwargs):
            if random.choice([True, False]):
                self.logger.info(f'use sync_client for {func_name}')
                return getattr(self.sync_client, func_name)(*args, **kwargs)
            else:
                self.logger.info(f'use async_client for {func_name}')
                return asyncio.run(self._run_async_client_func(func_name, *args, **kwargs))

        return generated_func

tablestore.OTSClient = RandomOTSClient