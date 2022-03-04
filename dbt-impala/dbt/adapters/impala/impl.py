from dbt.adapters.sql import SQLAdapter
from dbt.adapters.impala import ImpalaConnectionManager


class ImpalaAdapter(SQLAdapter):
    ConnectionManager = ImpalaConnectionManager

    @classmethod
    def date_function(cls):
        return 'now()'

