from contextlib import contextmanager
from dataclasses import dataclass

import dbt.exceptions

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager

from typing import Optional

from dbt.contracts.connection import AdapterResponse

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.connection import AdapterResponse

import impala.dbapi

DEFAULT_IMPALA_PORT = 21050

@dataclass
class ImpalaCredentials(Credentials):
    host: str
    port: int = DEFAULT_IMPALA_PORT
    username: Optional[str] = None
    password: Optional[str] = None
    schema: str
    database: str

    _ALIASES = {
        'dbname':'database',
        'pass':'password',
        'user':'username'
    }

    @property
    def type(self):
        return 'impala'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        return ('host', 'port', 'database', 'schema', 'username')

    @property
    def unique_field(self) -> str:
        # adapter anonymous adoption
        return self.host


class ImpalaConnectionManager(SQLConnectionManager):
    TYPE = 'impala'

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except impala.dbapi.DatabaseError as exc:
            logger.debug('dbt-imapla error: {}'.format(str(e)))
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            raise dbt.exceptions.RuntimeException(str(exc))

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            handle = impala.dbapi.connect(
                host=credentials.host,
                port=credentials.port,
            )
            connection.state = 'open'
            connection.handle = handle
        except:
            logger.debug("Connection error")
            connection.state = 'fail'
            connection.handle = None
            pass

        return connection

    @classmethod
    def get_response(cls, cursor):
        message = 'OK'
        return AdapterResponse(
            _message=message
        )

    def cancel(self, connection):
        connection.handle.close()
