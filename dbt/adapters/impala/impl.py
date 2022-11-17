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

import json
import re
from collections import OrderedDict
from concurrent.futures import Future, as_completed
from typing import Any, Dict, Iterable, List, Tuple

import agate
import dbt.exceptions
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.clients import agate_helper
from dbt.clients.agate_helper import ColumnTypeBuilder, NullableAgateType, _NullMarker
from dbt.events import AdapterLogger
from dbt.exceptions import warn_or_error
from dbt.utils import executor

import dbt.adapters.impala.cloudera_tracking as tracker
from dbt.adapters.impala import ImpalaConnectionManager
from dbt.adapters.impala.column import ImpalaColumn
from dbt.adapters.impala.relation import ImpalaRelation

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
            if len(row) != 2:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from "show table extended ...", '
                    f'got {len(row)} values, expected 4'
                )
            _identifier = row[0]
            _rel_type   = row[1]

            relation = self.Relation.create(
                database=None, 
                schema=schema_relation.schema,
                identifier=_identifier,
                type=_rel_type,
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
            try:
                rows: List[agate.Row] = super().get_columns_in_relation(relation)
                columns = self.parse_describe_extended(relation, rows)
            except dbt.exceptions.RuntimeException as e:
                # impala would throw error when table doesn't exist
                errmsg = getattr(e, "msg", "")
                if "Table or view not found" in errmsg or "NoSuchTableException" in errmsg:
                    pass
                else:
                    raise e

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
        pos = ImpalaAdapter.find_table_information_separator(dict_rows)  # ensure that the class method is called

        # Remove rows that start with a hash, they are comments
        rows = [
            row for row in raw_rows[0:pos]
            if not row['name'].startswith('#') and not row['name'] == ''
        ]
        # trim the fields so that these are clean key,value pairs and metadata.get() correctly returns the values
        metadata = {
            col['name'].split(":")[0].strip(): col['type'].strip() for col in raw_rows[pos + 1:]
            if col['name'] and not col['name'].startswith('#') and not col['name'] == '' and col['type']
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

    def _merged_column_types(
        tables: List[agate.Table]
    ) -> Dict[str, agate.data_types.DataType]:
        # this is a lot like agate.Table.merge, but with handling for all-null
        # rows being "any type".
        new_columns: ColumnTypeBuilder = ColumnTypeBuilder()
        for table in tables:
            for i in range(len(table.columns)):
                column_name: str = table.column_names[i]
                column_type: NullableAgateType = table.column_types[i]
                # avoid over-sensitive type inference
                if all(x is None for x in table.columns[column_name]):
                    column_type = _NullMarker()
                
                new_columns[column_name] = column_type

        return new_columns.finalize()

    def _merge_tables(tables: List[agate.Table]) -> agate.Table:
        new_columns = ImpalaAdapter._merged_column_types(tables)
        column_names = tuple(new_columns.keys())
        column_types = tuple(new_columns.values())

        rows: List[agate.Row] = []
        for table in tables:
            if (
                table.column_names == column_names and
                table.column_types == column_types
            ):
                rows.extend(table.rows)
            else:
                for row in table.rows:
                    data = [row.get(name, None) for name in column_names]
                    rows.append(agate.Row(data, column_names))
        # _is_fork to tell agate that we already made things into `Row`s.
        return agate.Table(rows, column_names, column_types, _is_fork=True)

    def _catch_as_completed(
        futures  # typing: List[Future[agate.Table]]
    ) -> Tuple[agate.Table, List[Exception]]:

        # catalogs: agate.Table = agate.Table(rows=[])
        tables: List[agate.Table] = []
        exceptions: List[Exception] = []

        for future in as_completed(futures):
            exc = future.exception()
            # we want to re-raise on ctrl+c and BaseException
            if exc is None:
                catalog = future.result()
                tables.append(catalog)
            elif (
                isinstance(exc, KeyboardInterrupt) or
                not isinstance(exc, Exception)
            ):
                raise exc
            else:
                warn_or_error(
                    f'Encountered an error while generating catalog: {str(exc)}'
                )
                # exc is not None, derives from Exception, and isn't ctrl+c
                exceptions.append(exc)

        return ImpalaAdapter._merge_tables(tables), exceptions

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)


        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                            self, schema,
                            self._get_one_catalog, info, [schema], manifest
                        ))
            catalogs, exceptions = catch_as_completed(futures) # call the default implementation 

        return catalogs, exceptions

    def _get_datatype(self, col_type):
        defaultType = agate.data_types.Text(null_values=('null', ''))

        datatypeMap = {
            'int': agate.data_types.Number(null_values=('null', '')),
            'double': agate.data_types.Number(null_values=('null', '')),
            'timestamp': agate.data_types.DateTime(null_values=('null', ''), datetime_format='%Y-%m-%d %H:%M:%S'),
            'date': agate.data_types.Date(null_values=('null', ''), date_format='%Y-%m-%d'),
            'boolean': agate.data_types.Boolean(true_values=('true',), false_values=('false',), null_values=('null', '')),
            'text': defaultType,
            'string': defaultType
        }

        try:
            dt = datatypeMap[col_type]

            if (dt == None): 
                return defaultType
            else:
                return dt
        except:
            return defaultType

    def _get_one_catalog(
        self, information_schema, schemas, manifest
    ) -> agate.Table:

        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in ImpalaAdapter._get_one_catalog, found '
                f'{schemas}'
            )

        schema = list(schemas)[0]
        
        columns: List[Dict[str, Any]] = []

        relation_list = self.list_relations(None, schema)

        for relation in relation_list:
            columns.extend(self._get_columns_for_catalog(relation))

        if len(columns) > 0:
            text_types = agate_helper.build_type_tester(["table_database", "table_schema", "table_name"])
        else:
            text_types = []

        return agate.Table.from_object(
            columns,
            column_types = text_types
        )

    def _get_columns_for_catalog(
        self, relation: ImpalaRelation
    ) -> Iterable[Dict[str, Any]]:
        columns = self.get_columns_in_relation(relation)

        for column in columns:
            # convert ImpalaColumns into catalog dicts
            as_dict = column.to_column_dict()
            
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = None

            yield as_dict
 
    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # We override this from base dbt adapter because impala doesn't need to escape interval 
        # duration string like postgres/redshift.
        return f"{add_to} + interval {number} {interval}"

    def debug_query(self) -> None:
        self.execute("select 1 as id")
        username = ""

        # query user permissions where available
        try:
            username = self.config.credentials.username
            if not username: # username is not available when auth_type is insecure or kerberos
                logger.debug("No username available to fetch permissions")
                payload = {
                    "event_type": "dbt_impala_debug_and_fetch_permissions",
                    "permissions": "NA",
                }
                tracker.track_usage(payload)
            else:
                sql_query = "show grant user `" + username + "` on server"
                _, table = self.execute(sql_query, True, True)
                permissions_object = []
                json_funcs = [c.jsonify for c in table.column_types]

                for row in table.rows:
                    values = tuple(json_funcs[i](d) for i, d in enumerate(row))
                    permissions_object.append(OrderedDict(zip(row.keys(), values)))

                permissions_json = permissions_object

                payload = {
                    "event_type": "dbt_impala_debug_and_fetch_permissions",
                    "permissions": permissions_json,
                }
                tracker.track_usage(payload)
        except Exception as ex:
            logger.error(
                f"Failed to fetch permissions for user: {username}. Exception: {ex}"
            )
            self.connections.get_thread_connection().handle.close()

        # query warehouse version
        try:
            sql_query = "select version()"
            _, table = self.execute(sql_query, True, True)
            version_object = []
            json_funcs = [c.jsonify for c in table.column_types]

            for row in table.rows:
                values = tuple(json_funcs[i](d) for i, d in enumerate(row))
                version_object.append(OrderedDict(zip(row.keys(), values)))

            version_json = version_object

            payload = {
                "event_type": "dbt_impala_warehouse",
                "warehouse_version": version_json,
            }
            tracker.track_usage(payload)
        except Exception as ex:
            logger.error(
                f"Failed to fetch warehouse version. Exception: {ex}"
            )

        self.connections.get_thread_connection().handle.close()

    ###
    # Methods about grants
    ###
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """Translate the result of `show grants` (or equivalent) to match the
        grants which a user would configure in their project.

        Ideally, the SQL to show grants should also be filtering:
        filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
        If that's not possible in SQL, it can be done in this method instead.

        :param grants_table: An agate table containing the query result of
            the SQL returned by get_show_grant_sql
        :return: A standardized dictionary matching the `grants` config
        :rtype: dict
        """
        unsupported_privileges = ["INDEX", "READ", "WRITE"]

        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["grantor"]
            privilege = row["privilege"]

            # skip unsupported privileges
            if privilege in unsupported_privileges:
                continue

            if privilege in grants_dict.keys():
                grants_dict[privilege].append(grantee)
            else:
                grants_dict.update({privilege: [grantee]})
        return grants_dict

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "insert_overwrite"]
        