import pytest
from dbt.adapters.contracts.connection import AdapterResponse


@pytest.mark.usefixtures("project")
class TestAdapterResponseImpala:
    def test_select_rowcount_from_buffer(self, adapter):
        ctas_sql = """
        create table dbt_test_ctas as
        select 1 as id union all select 2 as id union all select 3 as id
        """

        with adapter.connection_named("test_ctas"):
            response, _ = adapter.execute(ctas_sql, fetch=False)

        assert isinstance(response, AdapterResponse)
        assert response.rows_affected == 3
        assert response._message == "OK (3 rows)"

    def test_dml_insert_rows(self, adapter):
        create_sql = """
        create table if not exists dbt_test_response_insert (
            id int
        )
        """

        insert_sql = """
        insert into dbt_test_response_insert values
        (1),
        (2)
        """

        with adapter.connection_named("test_dml"):
            adapter.execute(create_sql, fetch=False)

            response, _ = adapter.execute(insert_sql, fetch=False)

        assert isinstance(response, AdapterResponse)
        assert response.rows_affected == 2
        assert response._message == "OK (2 rows)"
