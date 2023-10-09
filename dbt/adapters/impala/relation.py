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

from dataclasses import dataclass, field
from typing import Optional

from dbt.adapters.base.relation import BaseRelation, Policy

import dbt.adapters.impala.cloudera_tracking as tracker

from dbt.contracts.relation import (
    RelationType
)

GET_RELATION_TYPE_MACRO_NAME = "get_relation_type"

@dataclass
class ImpalaQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class ImpalaIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class ImpalaRelation(BaseRelation):
    quote_policy: ImpalaQuotePolicy = field(default_factory=lambda: ImpalaQuotePolicy())
    include_policy: ImpalaIncludePolicy = field(default_factory=lambda: ImpalaIncludePolicy())
    quote_character: str = None
    information: str = None
    adapter: str = None

    def __post_init__(self):
        if self.type:
            tracker.track_usage(
                {
                    "event_type": tracker.TrackingEventType.MODEL_ACCESS,
                    "model_name": self.render(),
                    "model_type": self.type,
                    "incremental_strategy": "",
                }
            )

    def __getattribute__(self, name):
        if name == 'type':
            type_info = object.__getattribute__(self, name)
            if type_info:
                return type_info
            elif self.adapter:
                result = self.adapter.execute_macro(GET_RELATION_TYPE_MACRO_NAME, 
                     kwargs= {"relation": ImpalaRelation.create(
                        database=self.database, 
                        schema=self.schema,
                        identifier=self.identifier,
                        information=self.identifier,
                    )})
                return result
            return None
        return object.__getattribute__(self, name)

    def render(self):
        return super().render()

    def log_relation(self, incremental_strategy):
        if self.type:
            tracker.track_usage(
                {
                    "event_type": tracker.TrackingEventType.INCREMENTAL,
                    "model_name": self.render(),
                    "model_type": self.type,
                    "incremental_strategy": incremental_strategy,
                }
            )

    @classmethod
    def create(
        cls,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
        type: Optional[RelationType] = None,
        **kwargs,
    ):
        kwargs.update(
            {
                "path": {
                    "database": database,
                    "schema": schema,
                    "identifier": identifier,
                },
                "type": type,
                "adapter": kwargs['adapter'] if 'adapter' in kwargs else None
            }
        )
        return cls.from_dict(kwargs)

    def new_copy(self, name, identifier):
        new_relation = ImpalaRelation.create(
            database=None,  # since include policy of database is False, this should be None
            schema=name,
            identifier=identifier,
            information=identifier,
        )

        return new_relation
