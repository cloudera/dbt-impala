import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

impala_ldap = {
        'type': 'impala',
        'threads': 4,
        'schema': 'dbt_adapter_test',
        'host':  os.getenv('IMPALA_HOST'),
        'http_path': os.getenv('IMPALA_HTTP_PATH'),
        'port': int(os.getenv('IMPALA_PORT')),
        'auth_type': 'ldap',
        'use_http_transport': True,
        'use_ssl': True,
        'user': os.getenv('IMPALA_USER'),
        'password': os.getenv('IMPALA_PASSWORD')
    }

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return impala_ldap

