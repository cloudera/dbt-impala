#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt_impala"
# make sure this always matches dbt/adapters/dbt-impala/__version__.py
package_version = "1.0.1"
description = """The dbt-impala adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description="Impala adapter for DBT",
    long_description="Impala adapter for DBT",
    author="V. Ganesh",
    author_email="ganesh.venkateshwara@cloudera.com",
    url="<INSERT URL HERE>",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-core==1.0.1",
        "impyla"
    ]
)
