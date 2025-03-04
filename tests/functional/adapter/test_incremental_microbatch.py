# Copyright 2025 Cloudera Inc.
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

from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch

_input_timestamp_modified_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id,  to_timestamp('2020-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as event_time
union all
select 2 as id,  to_timestamp('2020-01-02 00:00:00', 'yyyy-MM-dd HH:mm:ss') as event_time
union all
select 3 as id, to_timestamp('2020-01-03 00:00:00', 'yyyy-MM-dd HH:mm:ss') as event_time
"""

_microbatch_uniqueid_modified_model_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', partition_by='date_day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)) }}
select *, to_date(event_time) as date_day from {{ ref('input_model') }}
"""


class TestImpalaMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_timestamp_modified_model_sql

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_uniqueid_modified_model_sql

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, to_timestamp('2020-01-04 00:00:00', 'yyyy-MM-dd HH:mm:ss')), (5, to_timestamp('2020-01-05 00:00:00', 'yyyy-MM-dd HH:mm:ss'))"
