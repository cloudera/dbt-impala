import pytest
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    rm_file,
    write_file,
    check_table_does_not_exist,
    run_dbt_and_capture,
)
from dbt.tests.adapter.concurrency.test_concurrency import BaseConcurrency, seeds__update_csv


class TestConcurrencyImpala(BaseConcurrency):
    def test_concurrency_impala(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "VIEW_MODEL"])
        check_relations_equal(project.adapter, ["SEED", "DEP"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_A"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_B"])

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds__update_csv, project.project_root + "/seeds", "seed.csv")
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["SEED", "VIEW_MODEL"])
        check_relations_equal(project.adapter, ["SEED", "DEP"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_A"])
        check_relations_equal(project.adapter, ["SEED", "TABLE_B"])

    def test_concurrency(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "`skip`")

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds__update_csv, project.project_root, "seeds", "seed.csv")

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "`skip`")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
