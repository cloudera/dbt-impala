import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="cdh_endpoint", type=str)


# Using @pytest.mark.skip_profile('apache_spark') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "cdh_endpoint":
        target = cdh_target()
    elif profile_type == "dwx_endpoint":
        target = dwx_target()
    elif profile_type == "local_endpoint":
        target = local_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def cdh_target():
    return {
        "type": "impala",
        "threads": 4,
        "schema": os.getenv("IMPALA_SCHEMA") or "dbt_adapter_test",
        "host": os.getenv("IMPALA_HOST"),
        "port": int(os.getenv("IMPALA_PORT")),
        "auth_type": "insecure",
        "use_http_transport": False,
    }


def dwx_target():
    return {
        "type": "impala",
        "threads": 4,
        "auth_type": "ldap",
        "use_http_transport": True,
        "use_ssl": True,
        "host": os.getenv("IMPALA_HOST"),
        "port": int(os.getenv("IMPALA_PORT")),
        "schema": os.getenv("IMPALA_SCHEMA") or "dbt_adapter_test",
        "user": os.getenv("IMPALA_USER"),
        "password": os.getenv("IMPALA_PASSWORD"),
        "http_path": os.getenv("IMPALA_HTTP_PATH") or "cliservice",
    }


def local_target():
    return {
        "type": "impala",
        "threads": 1,
        "host": "localhost",
        "port": 21050,
        "auth_type": "insecure",
        "use_http_transport": False,
        "use_ssl": False,
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip("skipped on '{profile_type}' profile")
