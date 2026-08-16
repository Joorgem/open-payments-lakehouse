# tests/integration/test_postgres.py
import psycopg
import pytest

# BOTH MARKERS. This needs only the container, which is exactly what `postgres` means, and
# a CI job running `-m postgres` should include the liveness check before the tests that
# assume liveness. It carried `integration` alone until the F-DB Task 3 review.
pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def test_postgres_reachable():
    with psycopg.connect("host=localhost port=5433 dbname=opl user=opl password=opl") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
