from dbt.tests.util import run_dbt, get_manifest, run_dbt_and_capture, write_file
import time

from tests.fixtures.jaffle_shop import JaffleShopProject


class TestBasic(JaffleShopProject):
    def test_basic(self, project):
        # test .dbtignore works
        write_file("models/ignore*.sql\nignore_folder", project.project_root, ".dbtignore")
        # Create the data from seeds
        results = run_dbt(["seed"])

        # Tests that the jaffle_shop project runs
        results = run_dbt(["run"])
        assert len(results) == 5
        manifest = get_manifest(project.project_root)
        assert "model.jaffle_shop.orders" in manifest.nodes

    def test_execution_time_format_is_humanized(self, project):
        # Create the data from seeds
        run_dbt(["seed"])
        # first run
        run_dbt(["run"])
        time.sleep(15)
        # second run
        _, log_output = run_dbt_and_capture(["run"])

        assert " in 0 hours " in log_output
        assert " minutes and " in log_output
        assert " seconds" in log_output
