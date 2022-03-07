from dbt.adapters.sql import SQLAdapter
from dbt.adapters.impala import ImpalaConnectionManager

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

class ImpalaAdapter(SQLAdapter):
    Relation = ImpalaRelation
    Column = ImpalaColumn
    ConnectionManager = ImpalaConnectionManager

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
                schema=schema_relation.schema,
                identifier=_identifier,
                type=rel_type,
                information=_identifier,
            )
            relations.append(relation)

        return relations