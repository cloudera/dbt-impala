# Copyright 2026 Cloudera Inc.
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

import os

import pytest

from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture

_merge_iceberg_v2_sql = """
{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="id",
    table_type="iceberg",
    tbl_properties="('format-version'='2')"
  )
}}
select cast(id as int) as id, cast(name as string) as name, cast(amount as int) as amount
from (
    {% if is_incremental() %}
    select 1 as id, 'alice' as name, 11 as amount
    union all
    select 3 as id, 'carol' as name, 30 as amount
    {% else %}
    select 1 as id, 'alice' as name, 10 as amount
    union all
    select 2 as id, 'bob' as name, 20 as amount
    {% endif %}
) src
""".strip()

_merge_missing_unique_key_sql = """
{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    table_type="iceberg",
    tbl_properties="('format-version'='2')"
  )
}}
select cast(1 as int) as id, cast('a' as string) as name
""".strip()

_merge_non_iceberg_sql = """
{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="id"
  )
}}
select cast(1 as int) as id, cast('a' as string) as name
""".strip()

_merge_kudu_sql = """
{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="id",
    stored_as="kudu",
    primary_key="(id)",
    partition_by="HASH (id) PARTITIONS 2"
  )
}}
select cast(id as int) as id, cast(name as string) as name, cast(amount as int) as amount
from (
    {% if is_incremental() %}
    select 1 as id, 'alice' as name, 11 as amount
    union all
    select 3 as id, 'carol' as name, 30 as amount
    {% else %}
    select 1 as id, 'alice' as name, 10 as amount
    union all
    select 2 as id, 'bob' as name, 20 as amount
    {% endif %}
) src
""".strip()

_merge_kudu_missing_pk_sql = """
{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="id",
    stored_as="kudu"
  )
}}
select cast(1 as int) as id, cast('a' as string) as name
""".strip()


class TestMergeIcebergV2Impala:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": _merge_iceberg_v2_sql}

    def test_merge_updates_and_inserts(self, project):
        run_dbt(["run", "--full-refresh"])
        relation = relation_from_name(project.adapter, "merge_model")
        rows = project.run_sql(
            f"select id, name, amount from {relation} order by id", fetch="all"
        )
        assert [(int(r[0]), str(r[1]), int(r[2])) for r in rows] == [
            (1, "alice", 10),
            (2, "bob", 20),
        ]

        run_dbt(["run"])
        rows = project.run_sql(
            f"select id, name, amount from {relation} order by id", fetch="all"
        )
        assert [(int(r[0]), str(r[1]), int(r[2])) for r in rows] == [
            (1, "alice", 11),
            (2, "bob", 20),
            (3, "carol", 30),
        ]


class TestMergeRequiresUniqueKeyImpala:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": _merge_missing_unique_key_sql}

    def test_missing_unique_key_fails(self, project):
        _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert "requires a `unique_key` config" in stdout


class TestMergeRequiresIcebergOrKuduImpala:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": _merge_non_iceberg_sql}

    def test_plain_hive_fails(self, project):
        _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert "table_type='iceberg'" in stdout
        assert "stored_as='kudu'" in stdout


@pytest.mark.skipif(
    os.getenv(key="DISABLE_KUDU_TEST", default="true") == "true",
    reason="Kudu tests run when DISABLE_KUDU_TEST=false",
)
class TestMergeKuduImpala:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": _merge_kudu_sql}

    def test_kudu_upsert_updates_and_inserts(self, project):
        run_dbt(["run", "--full-refresh"])
        relation = relation_from_name(project.adapter, "merge_model")
        rows = project.run_sql(
            f"select id, name, amount from {relation} order by id", fetch="all"
        )
        assert [(int(r[0]), str(r[1]), int(r[2])) for r in rows] == [
            (1, "alice", 10),
            (2, "bob", 20),
        ]

        run_dbt(["run"])
        rows = project.run_sql(
            f"select id, name, amount from {relation} order by id", fetch="all"
        )
        assert [(int(r[0]), str(r[1]), int(r[2])) for r in rows] == [
            (1, "alice", 11),
            (2, "bob", 20),
            (3, "carol", 30),
        ]


@pytest.mark.skipif(
    os.getenv(key="DISABLE_KUDU_TEST", default="true") == "true",
    reason="Kudu tests run when DISABLE_KUDU_TEST=false",
)
class TestMergeKuduRequiresPrimaryKeyImpala:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": _merge_kudu_missing_pk_sql}

    def test_missing_primary_key_fails(self, project):
        _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert "requires a `primary_key` config" in stdout
