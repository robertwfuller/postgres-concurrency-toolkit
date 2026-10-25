#!/usr/bin/env python3
"""
postgres_concurrency_toolkit.py
Asset #20: High-Throughput Concurrent Job Queue Engine.
Uses 'FOR UPDATE SKIP LOCKED' to implement clean parallel job processing without Redis.
"""

import os
import time
import random
import psycopg2
from typing import Any, Dict, Optional

class PostgresJobQueue:
    def __init__(self, dsn: str = None):
        self.dsn = dsn or os.environ.get("PG_DSN", "postgresql://user:pass@localhost:5432/harness_test")

    def claim_next_job(self, worker_id: str, queue_name: str) -> Optional[Dict[str, Any]]:
        """
        Idempotently claims the next available queued job using 'FOR UPDATE SKIP LOCKED'
        to prevent parallel worker starvation or lock-waits.
        """
        sql = """
        UPDATE concurrent_jobs
        SET status = 'PROCESSING',
            claimed_by = %s,
            claimed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = (
            SELECT id
            FROM concurrent_jobs
            WHERE status = 'QUEUED' AND queue_name = %s
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, queue_name, payload;
        """
        conn = psycopg2.connect(self.dsn)
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (worker_id, queue_name))
            row = cursor.fetchone()
            conn.commit()
            if row:
                return {
                    "job_id": row[0],
                    "queue_name": row[1],
                    "payload": row[2]
                }
            return None
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    def calculate_jitter_backoff(self, attempt: int, base_ms: int = 300, max_ms: int = 5000) -> float:
        """
        Calculates exponential backoff with full jitter to mitigate database stampedes.
        Full Jitter formula: sleep = random(0, min(max_ms, base_ms * 2^attempt))
        """
        temp = min(max_ms, base_ms * (2 ** attempt))
        sleep_ms = random.randint(0, temp)
        return sleep_ms / 1000.0
