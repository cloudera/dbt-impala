import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

impala_ldap = {
        'type': 'impala',
        'threads': 4,
        'host':  os.getenv('IMPALA_HOST') or 'localhost',
        'http_path': os.getenv('IMPALA_HTTP_PATH') or '',
        'port': int(os.getenv('IMPALA_PORT') or 21050),
        'auth_type': os.getenv('AUTH_TYPE') or '',
        'use_http_transport': os.getenv('USE_HTTP_TRANSPORT') or False,
        'use_ssl': os.getenv('USE_SSL') or False,
        'user': os.getenv('IMPALA_USER') or '',
        'password': os.getenv('IMPALA_PASSWORD') or ''
    }

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return impala_ldap
