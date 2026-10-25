# PostgreSQL Concurrency Toolkit

A lightweight Python utility for PostgreSQL-backed job queues using `FOR UPDATE SKIP LOCKED` and exponential backoff with full jitter.

## Implemented Capabilities
* **Concurrent Job Claiming (`PostgresJobQueue.claim_next_job`):** Claims the highest-priority (`priority DESC, created_at ASC`) `QUEUED` job in a target queue and atomically transitions it to `PROCESSING` within a single transaction.
* **Full-Jitter Backoff (`PostgresJobQueue.calculate_jitter_backoff`):** Computes `random(0, min(max_ms, base_ms * 2**attempt)) / 1000.0` to reduce retry synchronization across workers.

## Required Schema (`concurrent_jobs`)
```sql
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
```

## Quickstart
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e . pytest
pytest -v
```
