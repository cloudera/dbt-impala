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

import time
import dbt.exceptions

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterRequiredConfig

from typing import Optional, Tuple, Any

from dbt.contracts.connection import Connection, AdapterResponse, ConnectionState

from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus

from dbt.events import AdapterLogger

import impala.dbapi
from impala.error import DatabaseError
from impala.error import HttpError
from impala.error import HiveServer2Error

import dbt.adapters.impala.__version__ as ver
import dbt.adapters.impala.cloudera_tracking as tracker

import json

from dbt.adapters.impala.__version__ import version as ADAPTER_VERSION

DEFAULT_IMPALA_HOST = "localhost"
DEFAULT_IMPALA_PORT = 21050
DEFAULT_MAX_RETRIES = 3

logger = AdapterLogger("Impala")


@dataclass
class ImpalaCredentials(Credentials):
    host: str = DEFAULT_IMPALA_HOST
    schema: str = None
    database: Optional[str] = None
    port: Optional[int] = DEFAULT_IMPALA_PORT
    username: Optional[str] = None
    password: Optional[str] = None
    auth_type: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    use_http_transport: Optional[bool] = True
    use_ssl: Optional[bool] = True
    http_path: Optional[str] = ""  # for supporting a knox proxy in ldap env
    usage_tracking: Optional[bool] = True  # usage tracking is enabled by default
    retries: Optional[int] = DEFAULT_MAX_RETRIES

    _ALIASES = {"dbname": "database", "pass": "password", "user": "username"}

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    def __post_init__(self):
        # impala classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Impala, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

        # set the usage tracking flag
        tracker.usage_tracking = self.usage_tracking
        # get platform information for tracking
        tracker.populate_platform_info(self, ver)
        # get dbt deployment env information for tracking
        tracker.populate_dbt_deployment_env_info()
        # generate unique ids for tracking
        tracker.populate_unique_ids(self)

    @property
    def type(self):
        return "impala"

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        return "host", "port", "schema", "username"

    @property
    def unique_field(self) -> str:
        # adapter anonymous adoption
        return self.host


class ImpalaConnectionWrapper:
    def __init__(self, handle):
        self.handle = handle
        self._cursor = self.handle.cursor()

    def cursor(self):
        if not self._cursor:
            self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            try:
                self._cursor.cancel()
            except OSError as exc:
                logger.debug(f"Exception while cancelling query: {exc}")

    def close(self):
        if self._cursor:
            try:
                self._cursor.close()
                self._cursor = None
            except OSError as exc:
                logger.debug(f"Exception while closing cursor: {exc}")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def execute(self, sql, bindings=None, configuration={}):
        result = self._cursor.execute(sql, bindings, configuration)
        return result

    @property
    def description(self):
        return self._cursor.description


class ImpalaConnectionManager(SQLConnectionManager):
    TYPE = "impala"

    impala_version = None

    def __init__(self, profile: AdapterRequiredConfig):
        super().__init__(profile)
        # generate profile related object for instrumentation.
        tracker.generate_profile_info(self)

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except HttpError as httpError:
            logger.debug(f"Authorization error: {httpError}")
            raise dbt.exceptions.DbtRuntimeError(
                "HTTP Authorization error: " + str(httpError) + ", please check your credentials"
            )
        except HiveServer2Error as servError:
            logger.debug(f"Server connection error: {servError}")
            raise dbt.exceptions.DbtRuntimeError(
                "Unable to establish connection to Impala server: " + str(servError)
            )
        except DatabaseError as dbError:
            logger.debug(f"Database connection error: {str(dbError)}")
            raise dbt.exceptions.DatabaseException("Database Connection error: " + str(dbError))
        except Exception as exc:
            logger.debug(f"Error running SQL: {sql}")
            raise dbt.exceptions.DbtRuntimeError(str(exc))

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        connection_ex = None

        auth_type = "insecure"

        try:
            connection_start_time = time.time()
            # the underlying dbapi supports retries, so this is directly used instead to support retries
            if (
                credentials.auth_type == "LDAP" or credentials.auth_type == "ldap"
            ):  # ldap connection
                custom_user_agent = "dbt/cloudera-impala-v" + ADAPTER_VERSION
                logger.debug(f"Using user agent: {custom_user_agent}")
                handle = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism="LDAP",
                    use_http_transport=credentials.use_http_transport,
                    user=credentials.username,
                    password=credentials.password,
                    use_ssl=credentials.use_ssl,
                    http_path=credentials.http_path,
                    retries=credentials.retries,
                    user_agent=custom_user_agent,
                )
                auth_type = "ldap"
            elif (
                credentials.auth_type == "GSSAPI"
                or credentials.auth_type == "gssapi"
                or credentials.auth_type == "kerberos"
            ):  # kerberos based connection
                handle = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism="GSSAPI",
                    kerberos_service_name=credentials.kerberos_service_name,
                    use_http_transport=credentials.use_http_transport,
                    use_ssl=credentials.use_ssl,
                    retries=credentials.retries,
                )
                auth_type = "kerberos"
            elif (
                credentials.auth_type == "PLAIN" or credentials.auth_type == "plain"
            ):  # plain type connection
                handle = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism="PLAIN",
                    use_ssl=credentials.use_ssl,
                    user=credentials.username,
                    password=credentials.password,
                    retries=credentials.retries,
                )
                auth_type = "plain"
            else:  # default, insecure connection
                handle = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    retries=credentials.retries,
                )
            connection_end_time = time.time()

            connection.state = ConnectionState.OPEN
            connection.handle = ImpalaConnectionWrapper(handle)

            ImpalaConnectionManager.fetch_impala_version(connection.handle)
        except Exception as ex:
            logger.error(f"Connection error {ex}")
            connection_ex = ex
            connection.state = ConnectionState.FAIL
            connection.handle = None
            connection_end_time = time.time()

        # track usage
        payload = {
            "event_type": tracker.TrackingEventType.OPEN,
            "auth": auth_type,
            "connection_state": connection.state,
            "elapsed_time": f"{connection_end_time - connection_start_time:.2f}",
        }

        if connection.state == ConnectionState.FAIL:
            payload["connection_exception"] = f"{connection_ex}"

        tracker.track_usage(payload)

        return connection

    @classmethod
    def close(cls, connection):
        try:
            # if the connection is in closed or init, there's nothing to do
            if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
                return connection

            connection_close_start_time = time.time()
            connection = super().close(connection)
            connection_close_end_time = time.time()

            payload = {
                "event_type": tracker.TrackingEventType.CLOSE,
                "connection_state": ConnectionState.CLOSED,
                "elapsed_time": "{:.2f}".format(
                    connection_close_end_time - connection_close_start_time
                ),
            }

            tracker.track_usage(payload)

            return connection
        except Exception as err:
            logger.debug(f"Error closing connection {err}")

    @classmethod
    def fetch_impala_version(cls, connection):
        if ImpalaConnectionManager.impala_version:
            return ImpalaConnectionManager.impala_version

        try:
            sql = "select version()"
            cursor = connection.cursor()
            cursor.execute(sql)

            res = cursor.fetchall()

            ImpalaConnectionManager.impala_version = res[0][0].split("RELEASE")[0].strip()

            tracker.populate_warehouse_info(
                {"version": ImpalaConnectionManager.impala_version, "build": res[0][0]}
            )
        except Exception as ex:
            # we couldn't get the impala warehouse version
            logger.debug(f"Cannot get impala version. Error: {ex}")
            ImpalaConnectionManager.impala_version = "NA"

            tracker.populate_warehouse_info(
                {"version": ImpalaConnectionManager.impala_version, "build": "NA"}
            )

        logger.debug(f"IMPALA VERSION {'ImpalaConnectionManager.impala_version'}")

    @classmethod
    def get_response(cls, cursor):
        message = "OK"
        return AdapterResponse(_message=message)

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
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        additional_info = {}
        if self.query_header:
            try:
                additional_info = json.loads(self.query_header.comment.query_comment.strip())
            except Exception as ex:  # silently ignore error for parsing
                additional_info = {}
                logger.debug(f"Unable to get query header {ex}")

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = f"{sql[:512]}..."
            else:
                log_sql = sql

            # track usage
            payload = {
                "event_type": tracker.TrackingEventType.START_QUERY,
                "sql": log_sql,
                "profile_name": self.profile.profile_name,
            }

            for key, value in additional_info.items():
                if key == "node_id":
                    payload["model_name"] = value
                else:
                    payload[key] = value

            tracker.track_usage(payload)

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # paramstyle parameter is needed for the datetime object to be correctly quoted when
            # running substitution query from impyla. this fix also depends on a patch for impyla:
            # https://github.com/cloudera/impyla/pull/486
            configuration = {"paramstyle": "format"}
            query_exception = None
            try:
                cursor.execute(sql, bindings, configuration)
                query_status = str(self.get_response(cursor))
            except Exception as ex:
                query_status = str(ex)
                query_exception = ex

            elapsed_time = time.time() - pre

            payload = {
                "event_type": tracker.TrackingEventType.END_QUERY,
                "sql": log_sql,
                "elapsed_time": f"{elapsed_time:.2f}",
                "status": query_status,
                "profile_name": self.profile.profile_name,
            }

            tracker.track_usage(payload)

            # re-raise query exception so that it propogates to dbt
            if query_exception:
                raise query_exception

            fire_event(
                SQLQueryStatus(
                    status=query_status,
                    elapsed=round(elapsed_time, 2),
                )
            )

            return connection, cursor
