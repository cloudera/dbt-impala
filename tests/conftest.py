import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

impala_ldap = {
        'type': 'impala',
        'threads': 5,
        'schema': 'dbttest',
        'host': 'coordinator-dbt-impala.dw-ciadev.cna2-sx9y.cloudera.site',
        'http_path': 'cliservice',
        'port': 443,
        'auth_type': 'ldap',
        'use_http_transport': True,
        'use_ssl': True,
        'user': os.getenv('IMPALA_USER'),
        'password': os.getenv('IMPALA_PASSWORD'),
    }

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return impala_ldap

