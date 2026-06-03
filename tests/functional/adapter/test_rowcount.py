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

import pytest
from dbt.tests.util import run_dbt

_incremental_insert_model_sql = """
{{ config(materialized='incremental') }}

{% if is_incremental() %}
    -- This SQL only executes during the SECOND run (The Insert phase)
    select 3 as id union all select 4 as id union all select 5 as id
{% else %}
    -- This SQL executes during the FIRST run (The Setup phase)
    select 1 as id union all select 2 as id
{% endif %}
"""


class TestRowcountImpala:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dbt_test_response_insert.sql": _incremental_insert_model_sql,
        }

    def test_rowcounts_and_messages(self, project):
        first_run = run_dbt(["run"])
        assert first_run[0].adapter_response["rows_affected"] == 2
        assert first_run[0].adapter_response["_message"] == "OK (2 rows)"

        second_run = run_dbt(["run"])
        assert second_run[0].adapter_response["rows_affected"] == 3
        assert second_run[0].adapter_response["_message"] == "OK (3 rows)"
