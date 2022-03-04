from dbt.adapters.sql import SQLAdapter
from dbt.adapters.dbt-impala import ImpalaConnectionManager


class ImpalaAdapter(SQLAdapter):
    ConnectionManager = ImpalaConnectionManager
