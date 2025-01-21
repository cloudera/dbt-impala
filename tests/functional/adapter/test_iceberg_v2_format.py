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

import pytest

from .test_iceberg_format import (
    TestSimpleMaterializationsIcebergFormatImpala,
    TestIncrementalIcebergFormatImpala,
)

from dbt.tests.adapter.basic.files import (
    model_base,
    model_incremental,
    base_view_sql,
    schema_base_yml,
)

iceberg_base_table_sql = (
    """
{{
  config(
    materialized="table",
    table_type="iceberg",
    tbl_properties="('format-version'='2')"
  )
}}""".strip()
    + model_base
)

iceberg_base_materialized_var_sql = (
    """
{{
  config(
    materialized=var("materialized_var", "table"),
    table_type="iceberg",
    tbl_properties="('format-version'='2')"
  )
}}""".strip()
    + model_base
)

incremental_iceberg_sql = (
    """
 {{
    config(
        materialized="incremental",
        table_type="iceberg",
        tbl_properties="('format-version'='2')"
    )
}}
""".strip()
    + model_incremental
)

incremental_partition_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        partition_by="id_partition1",
        table_type="iceberg",
        tbl_properties="('format-version'='2')"
    )
}}
select *, id as id_partition1 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

incremental_multiple_partition_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        partition_by=["id_partition1", "id_partition2"],
        table_type="iceberg",
        tbl_properties="('format-version'='2')"
    )
}}
select *, id as id_partition1, id as id_partition2 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

insertoverwrite_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by="id_partition1",
        table_type="iceberg",
        tbl_properties="('format-version'='2')"
    )
}}
select *, id as id_partition1 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()


# For iceberg table formats, check_relations_equal util is not working as expected
# Impala upstream issue: https://issues.apache.org/jira/browse/IMPALA-12097
# Hence removing this check from unit tests
class TestSimpleMaterializationsIcebergV2FormatImpala(
    TestSimpleMaterializationsIcebergFormatImpala
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": iceberg_base_table_sql,
            "swappable.sql": iceberg_base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }


class TestIncrementalIcebergV2FormatImpala(TestIncrementalIcebergFormatImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_iceberg_sql,
            "schema.yml": schema_base_yml,
        }


class TestIncrementalPartitionIcebergV2FormatImpala(TestIncrementalIcebergFormatImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_partition_iceberg_sql,
            "schema.yml": schema_base_yml,
        }


class TestIncrementalMultiplePartitionIcebergV2FormatImpala(TestIncrementalIcebergFormatImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_multiple_partition_iceberg_sql,
            "schema.yml": schema_base_yml,
        }


class TestInsertoverwriteIcebergV2FormatImpala(TestIncrementalIcebergFormatImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": insertoverwrite_iceberg_sql,
            "schema.yml": schema_base_yml,
        }
