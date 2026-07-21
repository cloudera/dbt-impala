import pytest
from dbt.tests.adapter.constraints.fixtures import (
    my_model_view_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_with_quoted_column_name_sql,
    my_model_wrong_name_sql,
    my_model_wrong_order_sql,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintQuotedColumn,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalContractSqlHeader,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseTableContractSqlHeader,
    BaseViewConstraintsColumnsEqual,
)
from tests.functional.adapter.constraints.fixtures import (
    impala_my_model_wrong_order_depends_on_fk_sql,
    impala_foreign_key_model_sql,
    impala_model_contract_header_schema_yml,
    impala_model_contract_sql_header_sql,
    impala_model_incremental_contract_sql_header_sql,
    impala_model_quoted_column_schema_yml,
    impala_model_schema_yml,
    impala_my_model_incremental_wrong_order_sql,
    impala_my_model_incremental_wrong_name_sql,
    impala_constrained_model_schema_yml,
)

_expected_sql_impala = """
create table <model_identifier> (
    id TINYINT,
    color STRING,
    date_day STRING
) ;
insert into <model_identifier>
select id, color, date_day from (
    select 'blue' as color, 1 as id, '2019-01-01' as date_day
) as model_subq
"""

_expected_sql_quoted_column_impala = """
create table <model_identifier> (
    id TINYINT,
    `from` STRING ,
    date_day STRING
) ;
insert into <model_identifier>
    select id, `from`, date_day
    from (
        select
          'blue' as `from`,
           1 as id,
          '2019-01-01' as date_day
    ) as model_subq
"""

_expected_sql_model_constraints_impala = """
create table <model_identifier> (
    id INT,
    color STRING,
    date_day STRING,
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES <foreign_key_model_identifier> (id)
) ;
insert into <model_identifier>
    select id, color, date_day
    from (
        -- depends_on: <foreign_key_model_identifier>
        select
            'blue' as color,
            cast(1 as int) as id,
            '2019-01-01' as date_day
    ) as model_subq
"""


class ImpalaColumnEqualSetup:
    @pytest.fixture
    def string_type(self):
        return "STRING"

    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["cast(1 as int)", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as timestamp)", "timestamp", "TIMESTAMP"],
            ["true", "boolean", "BOOLEAN"],
            ["cast('1' as decimal(10,2))", "decimal(10,2)", "DECIMAL"],
        ]


class TestImpalaConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": impala_model_quoted_column_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_quoted_column_impala


class TestImpalaTableConstraintsColumnsEqual(
    ImpalaColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": impala_model_schema_yml,
        }


class TestImpalaViewConstraintsColumnsEqual(
    ImpalaColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": impala_model_schema_yml,
        }


class TestImpalaIncrementalConstraintsColumnsEqual(
    ImpalaColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": impala_my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": impala_my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": impala_model_schema_yml,
        }


class TestImpalaTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_sql,
            "constraints_schema.yml": impala_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_impala


class TestImpalaIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": impala_my_model_incremental_wrong_order_sql,
            "constraints_schema.yml": impala_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_impala


class TestImpalaTableContractSqlHeader(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": impala_model_contract_sql_header_sql,
            "constraints_schema.yml": impala_model_contract_header_schema_yml,
        }


class TestImpalaIncrementalContractSqlHeader(BaseIncrementalContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": impala_model_incremental_contract_sql_header_sql,
            "constraints_schema.yml": impala_model_contract_header_schema_yml,
        }


class TestImpalaModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": impala_my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": impala_foreign_key_model_sql,
            "constraints_schema.yml": impala_constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_model_constraints_impala
