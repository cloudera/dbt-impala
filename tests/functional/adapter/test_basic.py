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

import os
import pytest

from dbt.tests.util import (
    run_dbt,
    get_manifest,
    check_result_nodes_by_name,
    check_relations_equal,
    relation_from_name,
    check_relation_types
)

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

from dbt.tests.adapter.utils.base_utils import BaseUtils

from dbt.tests.adapter.basic.files import (
    config_materialized_table,
    base_table_sql,
    base_materialized_var_sql,
    schema_base_yml,
    incremental_sql,
    base_ephemeral_sql,
    ephemeral_table_sql,
    ephemeral_with_cte_sql,
    test_ephemeral_passing_sql,
    test_ephemeral_failing_sql,
    seeds_base_csv,
    generic_test_seed_yml,
    base_view_sql,
    generic_test_view_yml,
    generic_test_table_yml
)


class TestSimpleMaterializationsImpala(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 2

        # names exist in result nodes
        check_result_nodes_by_name(results, ["table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSingularTestsImpala(BaseSingularTests):
    pass


class TestSingularTestsEphemeralImpala(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": ephemeral_with_cte_sql,
            "passing_model.sql": config_materialized_table + test_ephemeral_passing_sql,
            "failing_model.sql": config_materialized_table + test_ephemeral_failing_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "passing.sql": test_ephemeral_passing_sql,
            "failing.sql": test_ephemeral_failing_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "singular_tests_ephemeral",
        }

    def test_singular_tests_ephemeral(self, project):
        # check results from seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # Check results from test command
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 2
        check_result_nodes_by_name(results, ["passing", "failing"])

        # Check result status
        for result in results:
            if result.node.name == "passing":
                assert result.status == "pass"
            elif result.node.name == "failing":
                assert result.status == "fail"

        # check results from run command
        results = run_dbt()
        assert len(results) == 2
        check_result_nodes_by_name(results, ["failing_model", "passing_model"])

class TestEmptyImpala(BaseEmpty):
    pass

class TestEphemeralImpala(BaseEphemeral):
   
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": base_ephemeral_sql,
            "table_model.sql": ephemeral_table_sql,
            "schema.yml": schema_base_yml,
        }

    def test_ephemeral(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # run command
        results = run_dbt(["run"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["table_model"])

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations equal
        check_relations_equal(project.adapter, ["base", "table_model"])

        # catalog node count
        catalog = run_dbt(["docs", "generate"])
        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        assert os.path.exists(catalog_path)
        assert len(catalog.nodes) == 2
        assert len(catalog.sources) == 1

        # manifest (not in original)
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 3
        assert len(manifest.sources) == 1


class TestIncrementalImpala(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental_test_model"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_test_model.sql": incremental_sql, "schema.yml": schema_base_yml}

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

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1


insertoverwrite_sql = """
 {{ config(materialized="incremental", incremental_strategy="insert_overwrite", partition_by="id_partition") }}
 select *, id as id_partition from {{ source('raw', 'seed') }}
 {% if is_incremental() %}
 where id > (select max(id) from {{ this }})
 {% endif %}
""".strip()


class TestInsertoverwriteImpala(TestIncrementalImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_test_model.sql": insertoverwrite_sql, "schema.yml": schema_base_yml}


incremental_single_partitionby_sql = """
 {{ config(materialized="incremental", partition_by="id_partition") }}
 select *, id as id_partition from {{ source('raw', 'seed') }}
 {% if is_incremental() %}
 where id > (select max(id) from {{ this }})
 {% endif %}
""".strip()


class TestIncrementalWithSinglePartitionKeyImpala(TestIncrementalImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_single_partitionby_sql,
            "schema.yml": schema_base_yml,
        }


incremental_multiple_partitionby_sql = """
 {{ config(materialized="incremental", partition_by=["id_partition1", "id_partition2"]) }}
 select *, id as id_partition1, id as id_partition2 from {{ source('raw', 'seed') }}
 {% if is_incremental() %}
 where id > (select max(id) from {{ this }})
 {% endif %}
""".strip()


class TestIncrementalWithMultiplePartitionKeyImpala(TestIncrementalImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_multiple_partitionby_sql,
            "schema.yml": schema_base_yml,
        }


class TestGenericTestsImpala(BaseGenericTests):
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "schema.yml": generic_test_seed_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
            "schema_table.yml": generic_test_table_yml,
        }

    def test_generic_tests(self, project):
        # seed command
        results = run_dbt(["seed"])

        # test command selecting base model
        results = run_dbt(["test", "-m", "base"])
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        assert len(results) == 1

        # test command, all tests
        results = run_dbt(["test"])
        assert len(results) == 2


@pytest.mark.skip(reason="Not working from the start ie v1.3.3")
class TestSnapshotCheckColsImpala(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip(reason="Not working from the start ie v1.3.3")
class TestSnapshotTimestampImpala(BaseSnapshotTimestamp):
    pass

@pytest.mark.skip(reason="Not working after views is disabled")
class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


class TestBaseUtilsImpala(BaseUtils):
    pass


incremental_not_schema_change_sql = """
{{ config(materialized="incremental", incremental_strategy="append") }}
select
    concat(concat('1', '-'), cast(current_timestamp() as string)) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
