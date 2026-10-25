#!/usr/bin/env python3
"""
postgres_concurrency_toolkit_test_suite.py
Scope: Unit tests and real PostgreSQL integration tests for PostgresJobQueue (Task 3: V-03 / G-03).
Covers:
  - Persisted job claims (status='PROCESSING', claimed_by, claimed_at)
  - Priority ordering (priority DESC, created_at ASC)
  - Independent connections skipping locked rows via FOR UPDATE SKIP LOCKED
  - Empty queue returning None
  - Rollback behavior on database error
"""

import json
import os
import uuid
import pytest
import psycopg2
from unittest.mock import patch
from postgres_concurrency_toolkit import PostgresJobQueue


class MockQueueCursor:
    def __init__(self, return_row=None, raise_error=None):
        self.return_row = return_row
        self.raise_error = raise_error
        self.executed_queries = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed_queries.append((sql, params))
        if self.raise_error:
            raise self.raise_error

    def fetchone(self):
        return self.return_row

    def close(self):
        self.closed = True


class MockQueueConnection:
    def __init__(self, return_row=None, raise_error=None):
        self.cursor_obj = MockQueueCursor(return_row=return_row, raise_error=raise_error)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


@patch("psycopg2.connect")
def test_claim_next_job_returns_claimed_row(mock_connect):
    row = ("job-101", "emergency_triage", {"address": "1420 Legacy Dr"})
    mock_conn = MockQueueConnection(return_row=row)
    mock_connect.return_value = mock_conn

    queue = PostgresJobQueue(dsn="postgresql://mock@localhost:5432/db")
    job = queue.claim_next_job(worker_id="worker-01", queue_name="emergency_triage")

    assert job == {"job_id": "job-101", "queue_name": "emergency_triage", "payload": {"address": "1420 Legacy Dr"}}
    assert mock_conn.committed is True
    assert mock_conn.rolled_back is False
    assert mock_conn.closed is True
    assert mock_conn.cursor_obj.closed is True


@patch("psycopg2.connect")
def test_claim_next_job_empty_queue_returns_none(mock_connect):
    mock_conn = MockQueueConnection(return_row=None)
    mock_connect.return_value = mock_conn

    queue = PostgresJobQueue(dsn="postgresql://mock@localhost:5432/db")
    assert queue.claim_next_job(worker_id="worker-01", queue_name="empty_queue") is None
    assert mock_conn.committed is True
    assert mock_conn.closed is True


@patch("psycopg2.connect")
def test_claim_next_job_rolls_back_on_db_error(mock_connect):
    mock_conn = MockQueueConnection(raise_error=psycopg2.OperationalError("connection lost"))
    mock_connect.return_value = mock_conn

    queue = PostgresJobQueue(dsn="postgresql://mock@localhost:5432/db")
    with pytest.raises(psycopg2.OperationalError):
        queue.claim_next_job(worker_id="worker-01", queue_name="emergency_triage")
    assert mock_conn.rolled_back is True
    assert mock_conn.committed is False
    assert mock_conn.closed is True


@pytest.mark.parametrize("attempt,base_ms,max_ms,expected_cap_sec", [
    (0, 300, 5000, 0.3),
    (1, 300, 5000, 0.6),
    (3, 300, 5000, 2.4),
    (6, 300, 5000, 5.0),
])
def test_calculate_jitter_backoff_bounds(attempt, base_ms, max_ms, expected_cap_sec):
    queue = PostgresJobQueue()
    for _ in range(25):
        val = queue.calculate_jitter_backoff(attempt=attempt, base_ms=base_ms, max_ms=max_ms)
        assert 0.0 <= val <= expected_cap_sec


@pytest.mark.skipif(not os.environ.get("TEST_PG_DSN"), reason="TEST_PG_DSN not set for live PostgreSQL integration")
def test_postgres_integration_skip_locked_priority_and_persistence():
    """
    Real PostgreSQL integration test exercising:
      1. Priority ordering (priority DESC, created_at ASC)
      2. FOR UPDATE SKIP LOCKED across two independent database connections
      3. Persisted status='PROCESSING' and claimed_by updates
      4. Empty queue returning None
    """
    dsn = os.environ["TEST_PG_DSN"]
    queue_name = f"q_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg2.connect(dsn)
    admin_conn.autocommit = True
    try:
        with admin_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS concurrent_jobs (
                    id VARCHAR(128) PRIMARY KEY,
                    queue_name VARCHAR(128) NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
                    priority INT NOT NULL DEFAULT 0,
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    claimed_by VARCHAR(128),
                    claimed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute(
                "INSERT INTO concurrent_jobs (id, queue_name, priority, payload) VALUES (%s, %s, %s, %s::jsonb), (%s, %s, %s, %s::jsonb);",
                (
                    f"{queue_name}_low", queue_name, 10, json.dumps({"tier": "low"}),
                    f"{queue_name}_high", queue_name, 90, json.dumps({"tier": "high"}),
                ),
            )

        # Hold a row lock on the highest-priority row in connection A (uncommitted transaction)
        lock_conn = psycopg2.connect(dsn)
        lock_conn.autocommit = False
        try:
            with lock_conn.cursor() as lcur:
                lcur.execute(
                    "SELECT id FROM concurrent_jobs WHERE id = %s FOR UPDATE;",
                    (f"{queue_name}_high",),
                )
                assert lcur.fetchone()[0] == f"{queue_name}_high"

                # While _high is locked by lock_conn, worker-B claiming from PostgresJobQueue MUST skip _high and claim _low
                q = PostgresJobQueue(dsn=dsn)
                claimed_while_locked = q.claim_next_job(worker_id="worker-B", queue_name=queue_name)
                assert claimed_while_locked is not None
                assert claimed_while_locked["job_id"] == f"{queue_name}_low"
        finally:
            lock_conn.rollback()
            lock_conn.close()

        # Now that lock_conn released its row lock on _high, worker-A claims _high
        q = PostgresJobQueue(dsn=dsn)
        claimed_after_release = q.claim_next_job(worker_id="worker-A", queue_name=queue_name)
        assert claimed_after_release is not None
        assert claimed_after_release["job_id"] == f"{queue_name}_high"

        # Empty queue now returns None
        assert q.claim_next_job(worker_id="worker-C", queue_name=queue_name) is None

        # Verify persisted database state directly
        with admin_conn.cursor() as cur:
            cur.execute(
                "SELECT id, status, claimed_by FROM concurrent_jobs WHERE queue_name = %s ORDER BY id;",
                (queue_name,),
            )
            rows = cur.fetchall()
            assert rows == [
                (f"{queue_name}_high", "PROCESSING", "worker-A"),
                (f"{queue_name}_low", "PROCESSING", "worker-B"),
            ]
    finally:
        with admin_conn.cursor() as cur:
            cur.execute("DELETE FROM concurrent_jobs WHERE queue_name = %s;", (queue_name,))
        admin_conn.close()
