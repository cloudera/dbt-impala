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
