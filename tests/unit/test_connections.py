from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.impala.connections import (
    ImpalaConnectionManager,
    ImpalaCredentials,
    _split_sql_statements,
)


JWT_PROFILE = {
    "type": "impala",
    "host": "coordinator.example.com",
    "port": 443,
    "schema": "dbt_test_schema",
    "auth_type": "jwt",
    "jwt": "aabbccddeeff",
    "use_http_transport": True,
    "use_ssl": True,
    "http_path": "cliservice",
    "retries": 3,
}


def _make_connection(credentials: ImpalaCredentials) -> Connection:
    return Connection(
        type="impala",
        name="test",
        state=ConnectionState.INIT,
        transaction_open=False,
        credentials=credentials,
    )


class TestSplitSqlStatements:
    def test_splits_statements_at_semicolons(self):
        sql = "SELECT 1; SELECT 2;\nSELECT 3"

        assert _split_sql_statements(sql) == ["SELECT 1", "SELECT 2", "SELECT 3"]

    def test_ignores_semicolons_in_line_comments(self):
        sql = "-- Old query; kept for reference\nSELECT 1; SELECT 2;"

        assert _split_sql_statements(sql) == [
            "-- Old query; kept for reference\nSELECT 1",
            "SELECT 2",
        ]

    def test_ignores_semicolons_in_block_comments(self):
        sql = "/* Another comment;\nstill a comment */ SELECT 1; SELECT 2;"

        assert _split_sql_statements(sql) == [
            "/* Another comment;\nstill a comment */ SELECT 1",
            "SELECT 2",
        ]

    def test_ignores_semicolons_in_quoted_strings(self):
        sql = "SELECT 'hello;world' AS message; SELECT 2;"

        assert _split_sql_statements(sql) == [
            "SELECT 'hello;world' AS message",
            "SELECT 2",
        ]

    def test_preserves_grouped_query_with_comments_and_quoted_semicolons(self):
        grouped_query = """
            WITH regional_totals AS (
                SELECT
                    region,
                    SUM(CASE
                        WHEN status = 'paid;confirmed' THEN amount
                        ELSE 0
                    END) AS total_amount
                FROM orders
                -- Ignore refunds; keep positive amounts
                WHERE amount > 0 AND (region = 'EU' OR region = 'US')
                GROUP BY region
                HAVING COUNT(*) > 1
            )
            SELECT region, total_amount
            FROM regional_totals
            /* Filter aggregated totals;
               preserve the grouping above */
            WHERE total_amount > 100
            ORDER BY total_amount DESC
        """.strip()
        sql = f"{grouped_query};\n--some comment\nSELECT COUNT(*) FROM orders;"

        assert _split_sql_statements(sql) == [
            grouped_query,
            "--some comment\nSELECT COUNT(*) FROM orders",
        ]


class TestImpalaJwtAuthentication:
    def test_jwt_credentials_from_profile(self):
        credentials = ImpalaCredentials.from_dict(JWT_PROFILE)

        assert isinstance(credentials, ImpalaCredentials)
        assert credentials.type == "impala"
        assert credentials.host == "coordinator.example.com"
        assert credentials.port == 443
        assert credentials.schema == "dbt_test_schema"
        assert credentials.auth_type == "jwt"
        assert credentials.jwt == "aabbccddeeff"
        assert credentials.use_http_transport is True
        assert credentials.use_ssl is True
        assert credentials.http_path == "cliservice"
        assert credentials.retries == 3
        assert credentials.database is None

    @patch.object(ImpalaConnectionManager, "fetch_impala_version")
    @patch("dbt.adapters.impala.connections.impala.dbapi.connect")
    def test_jwt_open_uses_correct_connect_kwargs(self, mock_connect, _mock_fetch_version):
        mock_connect.return_value = MagicMock()
        credentials = ImpalaCredentials.from_dict(JWT_PROFILE)
        connection = _make_connection(credentials)

        result = ImpalaConnectionManager.open(connection)

        mock_connect.assert_called_once_with(
            host="coordinator.example.com",
            port=443,
            auth_mechanism="JWT",
            jwt="aabbccddeeff",
            use_http_transport=True,
            use_ssl=True,
            http_path="cliservice",
            retries=3,
        )
        assert result.state == ConnectionState.OPEN
