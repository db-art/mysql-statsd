#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='mysql-statsd',
    version='0.1.5',
    description='Daemon that gathers statistics from MySQL and sends them to statsd.',
    long_description=readme + '\n\n' + history,
    author='Jasper Capel, Thijs de Zoete',
    author_email='jasper.capel@spilgames.com',
    url='https://github.com/spilgames/mysql-statsd',
    packages=[
        'mysql_statsd',
    ],
    package_dir={'mysql_statsd': 'mysql_statsd'},
    entry_points={
        'console_scripts': [
            'mysql_statsd = mysql_statsd:mysql_statsd.MysqlStatsd'
        ]
    },
    include_package_data=True,
    install_requires=[
        'MySQL-python==1.2.5',
        'pystatsd==0.1.10',
    ],
    license="BSD",
    zip_safe=False,
    keywords='mysql_statsd',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    test_suite='tests',
)
