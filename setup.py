#!/usr/bin/env python
import re
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = ''
with open('tablestore/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


with open('README.rst', 'rb') as f:
    readme = f.read().decode('utf-8')

setup(
    name='tablestore',
    version=version,
    description='Aliyun TableStore(OTS) SDK',
    long_description=readme,
    packages=['tablestore', 'tablestore.protobuf', 'tablestore.plainbuffer','tablestore.flatbuffer','tablestore.flatbuffer.timeseries','tablestore.flatbuffer.dataprotocol','dataprotocol'],
    package_dir={'tablestore.protobuf': 'tablestore/protobuf/','dataprotocol':'tablestore/flatbuffer/dataprotocol'},
    install_requires=['enum34>=1.1.6', 'protobuf>=3.20.0,<=5.27.4', 'urllib3>=1.14', 'certifi>=2016.2.28', 'future>=0.16.0', 'six>=1.11.0', 'flatbuffers>=22.9.24', 'numpy>=1.11.0', 'crc32c>=2.7.1'],
    include_package_data=True,
    url='https://cn.aliyun.com/product/ots',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12'
    ]
)

