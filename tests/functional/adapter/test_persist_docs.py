from dbt.tests.util import run_dbt
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class TestPersistDocsImpala(BasePersistDocs):
    def _assert_common_comments(self, *comments):
        for comment in comments:
            assert comment is not None
            assert "with double quotes" in comment
            assert "abc123" in comment
            assert "/* comment */" in comment
            if "\n" in comment:
                pass
            else:
                assert "statistics are made up" in comment or "reserved -- characters" in comment


class TestPersistDocsColumnMissingImpala(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnImpala(BasePersistDocsCommentOnQuotedColumn):
    pass
