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

from dbt.adapters.impala.connections import ImpalaConnectionManager
from dbt.adapters.impala.connections import ImpalaCredentials
from dbt.adapters.impala.column import ImpalaColumn
from dbt.adapters.impala.relation import ImpalaRelation
from dbt.adapters.impala.impl import ImpalaAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import impala


Plugin = AdapterPlugin(
    adapter=ImpalaAdapter,
    credentials=ImpalaCredentials,
    include_path=impala.PACKAGE_PATH)
