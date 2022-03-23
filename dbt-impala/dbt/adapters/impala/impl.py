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

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.impala import ImpalaConnectionManager

import re

from typing import List

import agate

import dbt.exceptions
from dbt.contracts.relation import RelationType

from dbt.adapters.impala.column import ImpalaColumn
from dbt.adapters.impala.relation import ImpalaRelation

from dbt.events import AdapterLogger

logger = AdapterLogger("Impala")

LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'

KEY_TABLE_OWNER = 'Owner'
KEY_TABLE_STATISTICS = 'Statistics'

class ImpalaAdapter(SQLAdapter):
    Relation = ImpalaRelation
    Column = ImpalaColumn
    ConnectionManager = ImpalaConnectionManager

    INFORMATION_COLUMNS_REGEX = re.compile(
        r"^ \|-- (.*): (.*) \(nullable = (.*)\b", re.MULTILINE)
    INFORMATION_OWNER_REGEX = re.compile(r"^Owner: (.*)$", re.MULTILINE)
    INFORMATION_STATISTICS_REGEX = re.compile(
        r"^Statistics: (.*)$", re.MULTILINE)

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "timestamp"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    def quote(self, identifier):
        return identifier # no quote

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "real" if decimals else "integer"
        
    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        
        return exists

    def list_schemas(self, database: str) -> List[str]:
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )
        
        schemas = []

        for row in results:
            _schema = row[0]
            schemas.append(_schema)

        return schemas

    def list_relations_without_caching(
        self, schema_relation: ImpalaRelation
    ) -> List[ImpalaRelation]:
        kwargs = {'schema_relation': schema_relation}

        try:
            results = self.execute_macro(
                LIST_RELATIONS_MACRO_NAME,
                kwargs=kwargs
            )
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, 'msg', '')
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 1:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from "show table extended ...", '
                    f'got {len(row)} values, expected 4'
                )
            _identifier = row[0]

            # TODO: the following is taken from spark, needs to see what is there in impala
            # TODO: this modification is not really right, need fix
            rel_type = RelationType.View \
                if 'view' in _identifier else RelationType.Table

            relation = self.Relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=_identifier,
                type=rel_type,
                information=_identifier,
            )
            relations.append(relation)

        return relations

    def get_columns_in_relation(self, relation: Relation) -> List[ImpalaColumn]:
        cached_relations = self.cache.get_relations(
            relation.database, relation.schema)
        cached_relation = next((cached_relation
                                for cached_relation in cached_relations
                                if str(cached_relation) == str(relation)),
                               None)
        columns = []
        if cached_relation and cached_relation.information:
            columns = self.parse_columns_from_information(cached_relation)

        # execute the macro and parse the data
        if not columns:
            rows: List[agate.Row] = super().get_columns_in_relation(relation)
            columns = self.parse_describe_extended(relation, rows)

        return columns

    def parse_describe_extended(
            self,
            relation: Relation,
            raw_rows: List[agate.Row]
    ) -> List[ImpalaColumn]:

        # TODO: this method is largely from dbt-spark, sample test with impala works (test_dbt_base: base)
        # need deeper testing 

        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE EXTENDED {{relation}} statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [
            row for row in raw_rows[0:pos]
            if not row['name'].startswith('#') and not row['name'] == ''
        ]
        metadata = {
            col['name']: col['type'] for col in raw_rows[pos + 1:]
        }

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = ImpalaColumn.convert_table_stats(raw_table_stats)

        return [ImpalaColumn(
            table_database=None,
            table_schema=relation.schema,
            table_name=relation.name,
            table_type=relation.type,
            table_owner=str(metadata.get(KEY_TABLE_OWNER)),
            table_stats=table_stats,
            column=column['name'],
            column_index=idx,
            dtype=column['type'],
        ) for idx, column in enumerate(rows)]

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if row['name'].startswith('# Detailed Table Information'):
                break
            pos += 1
        return pos

    def parse_columns_from_information(
            self, relation: ImpalaRelation
    ) -> List[ImpalaColumn]:

        # TODO: this method is largely from dbt-spark, sample test with impala works (test_dbt_base: base)
        # need deeper testing

        owner_match = re.findall(
            self.INFORMATION_OWNER_REGEX, relation.information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(
            self.INFORMATION_COLUMNS_REGEX, relation.information)
        columns = []
        stats_match = re.findall(
            self.INFORMATION_STATISTICS_REGEX, relation.information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = ImpalaColumn.convert_table_stats(raw_table_stats)
        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
            column = ImpalaColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats
            )
            columns.append(column)

        return columns
