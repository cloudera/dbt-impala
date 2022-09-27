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

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException

import dbt.adapters.impala.cloudera_tracking as tracker

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
    quote_policy: ImpalaQuotePolicy = ImpalaQuotePolicy()
    include_policy: ImpalaIncludePolicy = ImpalaIncludePolicy()
    quote_character: str = None
    information: str = None

    def __post_init__(self):
        if (self.type):
            tracker.track_usage({
                "event_type": "dbt_impala_model_access",
                "model_name": self.render(),
                "model_type": self.type,
                "incremental_strategy": ""
            })
    
    def render(self):
        return super().render()

    def log_relation(self, incremental_strategy):
        if (self.type):
            tracker.track_usage({
                "event_type": "dbt_impala_new_incremental",
                "model_name": self.render(),
                "model_type": self.type,
                "incremental_strategy": incremental_strategy
            })

    def new_copy(self, name, identifier):
        new_relation = ImpalaRelation.create(
                database=None, # since include policy of database is False, this should be None
                schema=name,
                identifier=identifier,
                information=identifier,
            )

        return new_relation
