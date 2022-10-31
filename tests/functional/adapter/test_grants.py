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
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants

from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    write_file,
    relation_from_name,
    get_connection,
)

class TestModelGrantsImpala(BaseModelGrants):
    def privilege_grantee_name_overrides(self):
        return {
            "select": "select",
            "insert": "insert",
        }

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)

        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True

user2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

class TestIncrementalGrantsImpala(BaseIncrementalGrants):
    def test_incremental_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_incremental_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        # doesn't work in impala as the regular command is 'show grant' and not 'show grants'
        # assert "grant " not in log_output  # with space to disambiguate from 'show grants'
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_incremental_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        # whether grants or revokes happened will vary by adapter
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            adapter.drop_schema(relation)

        # Incremental materialization, same config, rebuild now that table is missing
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # assert "grant " in log_output
        assert "revoke " not in log_output
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)

        for grant_key in actual_grants:
            if grant_key not in expected_grants:
                return False
        return True