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

from setuptools import find_namespace_packages, setup


with open('README.md') as f:
    long_description = f.read()

package_name = "dbt-impala"
# make sure this always matches dbt/adapters/dbt_impala/__version__.py
package_version = "1.0.5"
description = """The Impala adapter plugin for dbt"""

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
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.0.1",
        "impyla"
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
