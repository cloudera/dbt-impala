import pytest
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes

from tests.functional.adapter.column_types import fixtures
from tests.functional.adapter.column_types.fixtures import macro_test_is_type_sql


class TestHiveColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": macro_test_is_type_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": fixtures.model_sql, "schema.yml": fixtures.schema_yml}

    def test_run_and_test(self, project):
        self.run_and_test()
