#!/usr/bin/env python
import re
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.version_info[0] == 2:
    base_dir = 'py2'
elif sys.version_info[0] == 3:
    base_dir = 'py3'

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
    packages=['tablestore', 'tablestore.protobuf', 'tablestore.plainbuffer'],
    package_dir={'tablestore.protobuf': 'tablestore/protobuf/' + base_dir},
    install_requires=['enum34>=1.1.6', 'protobuf>=3.6.1', 'urllib3>=1.14', 'certifi>=2016.2.28', 'future>=0.16.0', 'six>=1.11.0'],
    include_package_data=True,
    url='https://cn.aliyun.com/product/ots',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)

