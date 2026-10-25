#!/usr/bin/env python3
"""
postgres_concurrency_toolkit_test_suite.py
Scope: Unit tests and optional disposable PostgreSQL integration tests for PostgresJobQueue (V-05 / G-05).
"""

import os
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
