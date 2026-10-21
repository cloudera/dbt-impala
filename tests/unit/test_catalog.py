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

from dbt.exceptions import DbtRuntimeError

from dbt.adapters.impala.impl import ImpalaAdapter


class _Relation:
    def __init__(self, schema, identifier):
        self.schema = schema
        self.identifier = identifier


def _column(table_name):
    return {
        "table_database": None,
        "table_schema": "analytics",
        "table_name": table_name,
        "column_name": "id",
        "column_type": "int",
        "column_index": 0,
    }


def test_catalog_skips_unloadable_table_and_keeps_the_rest():
    orders = _Relation("analytics", "orders")
    orders_bk = _Relation("analytics", "orders_bk")
    described = []

    adapter = ImpalaAdapter.__new__(ImpalaAdapter)
    adapter.list_relations = lambda database, schema: [orders, orders_bk]

    def columns_for(relation):
        described.append(relation.identifier)
        if relation.identifier == "orders_bk":
            raise DbtRuntimeError(
                "Could not load table analytics.orders_bk from catalog. File does not exist"
            )
        return [_column(relation.identifier)]

    adapter._get_columns_for_catalog = columns_for

    table = adapter._get_one_catalog(None, ["analytics"])

    assert described == ["orders", "orders_bk"]
    assert [row["table_name"] for row in table.rows] == ["orders"]


def test_catalog_still_raises_unrelated_errors():
    adapter = ImpalaAdapter.__new__(ImpalaAdapter)
    adapter.list_relations = lambda database, schema: [_Relation("analytics", "orders")]

    def columns_for(relation):
        raise DbtRuntimeError("Syntax error near line 1")

    adapter._get_columns_for_catalog = columns_for

    with pytest.raises(DbtRuntimeError, match="Syntax error"):
        adapter._get_one_catalog(None, ["analytics"])
