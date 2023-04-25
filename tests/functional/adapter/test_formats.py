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

from tests.functional.adapter.test_basic import (
    TestIncrementalImpala
)

from dbt.tests.adapter.basic.files import (
    schema_base_yml
)

incremental_iceberg_sql = """
 {{ config(materialized="incremental", iceberg=true) }}
 select * from {{ source('raw', 'seed') }}
 {% if is_incremental() %}
 where id > (select max(id) from {{ this }})
 {% endif %}
""".strip()

class TestIncrementalIcebergFormatImpala(TestIncrementalImpala):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_test_model.sql": incremental_iceberg_sql, "schema.yml": schema_base_yml}
 

