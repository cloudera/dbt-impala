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

import pytest

from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences

from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.grants.base_grants import BaseGrants

from dbt.tests.adapter.basic.files import (
    schema_base_yml,
    incremental_sql,
)


class TestSimpleMaterializationsImpala(BaseSimpleMaterializations):
    pass


class TestSingularTestsImpala(BaseSingularTests):
    pass


class TestSingularTestsEphemeralImpala(BaseSingularTestsEphemeral):
    pass


class TestEmptyImpala(BaseEmpty):
    pass


class TestEphemeralImpala(BaseEphemeral):
    pass


class TestIncrementalImpala(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental_test_model"}
    
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_test_model.sql": incremental_sql, "schema.yml": schema_base_yml}

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental_test_model"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental_test_model"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

class TestGenericTestsImpala(BaseGenericTests):
    pass


class TestSnapshotCheckColsImpala(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampImpala(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass

class TestBaseDocsImpala(BaseDocsGenerate):
    pass

class TestBaseDocsGenRefsImpala(BaseDocsGenReferences):
    pass

class TestBaseUtilsImpala(BaseUtils):
    pass

class TestBaseGrantsImpala(BaseGrants):
    pass 

