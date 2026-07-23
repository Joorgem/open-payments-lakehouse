# tests/integration/test_postgres.py
import psycopg
import pytest

pytestmark = pytest.mark.integration


def test_postgres_reachable():
    with psycopg.connect("host=localhost port=5433 dbname=opl user=opl password=opl") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
