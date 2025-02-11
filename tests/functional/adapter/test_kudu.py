# Copyright 2024 Cloudera Inc.
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
import os
from dbt.tests.util import run_dbt, relation_from_name, check_relations_equal

from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)

from dbt.tests.adapter.basic.files import (
    schema_base_yml,
    model_incremental,
)

from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization

pytestmark = pytest.mark.skipif(
    os.getenv(key="DISABLE_KUDU_TEST", default="true") == "true",
    reason="Kudu tests will be run when DISABLE_KUDU_TEST is set to false in test.env",
)

incremental_kudu_sql = (
    """
 {{
    config(
        materialized="incremental",
        stored_as="kudu",
        primary_key="(id)"
    )
}}
""".strip()
    + model_incremental
)


class TestIncrementalKudu(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental_test_model"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_test_model.sql": incremental_kudu_sql, "schema.yml": schema_base_yml}

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental_test_model"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental_test_model"])

        # run full-refresh and compare with base table again
        results = run_dbt(
            [
                "run",
                "--select",
                "incremental_test_model",
                "--full-refresh",
                "--vars",
                "seed_name: base",
            ]
        )
        assert len(results) == 1

        check_relations_equal(project.adapter, ["base", "incremental_test_model"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1


materialization_hash_partitionby_sql = """
 {{
    config(
        materialized="table",
        partition_by="HASH (id) PARTITIONS 2",
        stored_as="kudu",
        primary_key="(id)"
    )
}}
select * from {{ this.schema }}.seed
""".strip()


class TestMaterializationWithHashPartitionKudu(BaseTableMaterialization):
    @pytest.fixture(scope="class")
    def models(self):
        return {"materialized.sql": materialization_hash_partitionby_sql}


materialization_range_partitionby_sql = """
 {{
    config(
        materialized="table",
        partition_by="Range (id) (PARTITION VALUES < 5, PARTITION 5 <= VALUES)",
        stored_as="kudu",
        primary_key="(id)"
    )
}}
select * from {{ this.schema }}.seed
""".strip()


class TestMaterializationWithRangePartitionKudu(BaseTableMaterialization):
    @pytest.fixture(scope="class")
    def models(self):
        return {"materialized.sql": materialization_range_partitionby_sql}
