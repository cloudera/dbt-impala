#!/usr/bin/env python
# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
from setuptools import find_namespace_packages, setup

# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf8') as f:
    long_description = f.read()

# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version_dict():
    _version_path = os.path.join(
        this_directory, 'dbt', 'adapters', 'impala', '__version__.py'
    )
    _semver = r'''(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)'''
    _pre = r'''((?P<prekind>a|b|rc)(?P<pre>\d+))?'''
    _version_pattern = fr'''version\s*=\s*["']{_semver}{_pre}["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.groupdict()


# require a compatible minor version (~=), prerelease if this is a prerelease
def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = (parts["prekind"]+"1" if parts["prekind"] else "")
    return f"{minor}{pre}"

package_name = "dbt-impala"
# make sure this always matches dbt/adapters/dbt_impala/__version__.py
package_version = "1.3.0"
description = """The Impala adapter plugin for dbt"""

dbt_core_version = _get_dbt_core_version()

setup(
    name=package_name,
    version=package_version,
    description="Impala adapter for DBT",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Cloudera",
    author_email="innovation-feedback@cloudera.com",
    url="https://github.com/cloudera/dbt-impala",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    data_files=[('', ['dbt/adapters/impala/.env'])],
    include_package_data=True,
    install_requires=[
        'dbt-core~={}'.format(dbt_core_version),
        "impyla==0.18",
        "python-decouple>=3.6",
        "kerberos>=1.3.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: Apache Software License"
    ],
    zip_safe=False
)
