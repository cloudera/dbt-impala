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

from contextlib import contextmanager
from dataclasses import dataclass
from optparse import Option

import time
import dbt.exceptions

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager

from typing import Optional, Tuple, Any

from dbt.contracts.connection import Connection, AdapterResponse

from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus

from dbt.logger import GLOBAL_LOGGER as logger

import impala.dbapi

import json
import hashlib
import threading

DEFAULT_IMPALA_PORT = 21050

@dataclass
class ImpalaCredentials(Credentials):
    host: str
    database: Optional[str]
    port: Optional[int] = DEFAULT_IMPALA_PORT
    username: Optional[str] = None
    password: Optional[str] = None
    auth_type: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    use_http_transport: Optional[bool] = True
    use_ssl: Optional[bool] = True
    http_path: Optional[str] = ''  # for supporing a knox proxy in ldap env
    usage_tracking: Optional[bool] = True # usage tracking is enabled by default

    _ALIASES = {
        'dbname':'database',
        'pass':'password',
        'user':'username'
    }

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if 'database' not in data:
            data['database'] = None
        return data

    def __post_init__(self):
        # impala classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'On Impala, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = self.schema

    @property
    def type(self):
        return 'impala'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        return ('host', 'port', 'schema', 'username')

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

        auth_type = "insecure"

        try:
            if (credentials.auth_type == "LDAP" or credentials.auth_type == "ldap"): # ldap connection
                handle = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism='LDAP',
                    use_http_transport=credentials.use_http_transport,
                    user=credentials.username,
                    password=credentials.password,
                    use_ssl=credentials.use_ssl,
                    http_path=credentials.http_path
                )
                auth_type = "ldap"
            elif (credentials.auth_type == "GSSAPI" or credentials.auth_type == "gssapi" or credentials.auth_type == "kerberos"): # kerberos based connection
                handle = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism='GSSAPI',
                    kerberos_service_name=credentials.kerberos_service_name,
                    use_http_transport=credentials.use_http_transport,
                    use_ssl=credentials.use_ssl
                )
                auth_type = "kerberos"
            else: # default, insecure connection
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

        try:
            if (credentials.usage_tracking): 
               tracking_data = {}
               payload = {}
               payload["id"] = "dbt_impala_open"
               payload["unique_hash"] = hashlib.md5(credentials.host.encode()).hexdigest()
               payload["auth"] = auth_type
               payload["connection_state"] = connection.state

               tracking_data["data"] = payload

               the_track_thread = threading.Thread(target=track_usage, kwargs={"data": tracking_data})
               the_track_thread.start()
        except:
            logger.debug("Usage tracking error")

        return connection

    @classmethod
    def get_response(cls, cursor):
        message = 'OK'
        return AdapterResponse(
            _message=message
        )

    def cancel(self, connection):
        connection.handle.close()

    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # paramstlye parameter is needed for the datetime object to be correctly qouted when
            # running substitution query from impyla. this fix also depends on a patch for impyla:
            # https://github.com/cloudera/impyla/pull/486
            configuration = {}
            configuration['paramstyle'] = 'format'
            cursor.execute(sql, bindings, configuration)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre), 2)
                )
            )

            return connection, cursor

# usage tracking code - Cloudera specific 
def track_usage(data):
   import requests 
   from decouple import config

   SNOWPLOW_ENDPOINT = config('SNOWPLOW_ENDPOINT')
   SNOWPLOW_TIMEOUT  = int(config('SNOWPLOW_TIMEOUT')) # 10 seconds

   # prod creds
   headers = {'x-api-key': config('SNOWPLOW_API_KEY'), 'x-datacoral-environment': config('SNOWPLOW_ENNV'), 'x-datacoral-passthrough': 'true'}

   data = json.dumps([data])

   res = requests.post(SNOWPLOW_ENDPOINT, data = data, headers = headers, timeout = SNOWPLOW_TIMEOUT)

   return res

