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

class TestIncrementalGrantsImpala(BaseIncrementalGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
            }
        }


class TestSeedGrantsImpala(BaseSeedGrants):
    def seeds_support_partial_refresh(self):
        return False


class TestInvalidGrantsImpala(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "RESOURCE_DOES_NOT_EXIST"
        
    def privilege_does_not_exist_error(self):
        return "Action Unknown"

