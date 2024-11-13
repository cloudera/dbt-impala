# Copyright 2024 Cloudera Inc.
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

import pytest

from dbt_common.exceptions import (
    CompilationError,
    DbtRuntimeError,
    DbtDatabaseError,
    DbtBaseException,
)

from impala.error import DatabaseError, HttpError, HiveServer2Error


def raise_compilation_error(self):
    raise CompilationError("Compilation Error!! Please check if the syntax is right")


def raise_http_error(self):
    try:
        raise HttpError(code=500, message="Forbidden", body=None, http_headers={})
    except HttpError:
        raise DbtRuntimeError("An unexpected Http Error has occurred")


def raise_hive_server_error(self):
    try:
        raise HiveServer2Error()
    except HiveServer2Error:
        raise DbtRuntimeError("Internal server error in Hive, please check the Hive logs")


def raise_database_error(self):
    try:
        raise DatabaseError()
    except DatabaseError:
        raise DbtDatabaseError("Internal databse error in Hive, please check the Hive logs")


class TestException:
    def test_exception(self):
        with pytest.raises(CompilationError):
            raise_compilation_error(self)
        assert issubclass(CompilationError, DbtRuntimeError) == True

        with pytest.raises(DbtRuntimeError):
            raise_http_error(self)
        assert issubclass(DbtRuntimeError, DbtBaseException) == True

        with pytest.raises(DbtRuntimeError):
            raise_hive_server_error(self)
        assert issubclass(DbtRuntimeError, RuntimeError) == True

        with pytest.raises(DbtDatabaseError):
            raise_database_error(self)
        assert issubclass(DbtDatabaseError, DbtRuntimeError) == True
