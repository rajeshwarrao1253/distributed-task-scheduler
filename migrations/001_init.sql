-- =============================================================================
-- Distributed Task Scheduler - Database Schema
-- =============================================================================
-- Tables:
--   - jobs:         Active job records with full metadata
--   - job_history:  Audit trail of all job status transitions
--   - cron_jobs:    Recurring job definitions
-- Indexes:
--   - Optimized for queue polling (status + priority + scheduled_at)
--   - Lookup by job type and status for monitoring
--   - Cron job lookup for scheduler nodes
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- Extension for UUID generation
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- Enum: Job status
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE job_status AS ENUM (
            'pending',       -- Job is queued and waiting for execution
            'running',       -- Job is currently being processed
            'completed',     -- Job executed successfully
            'failed',        -- Job failed but may be retried
            'dead_letter',   -- Job failed and exceeded max retries
            'cancelled'      -- Job was manually cancelled
        );
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Table: jobs
-- Primary store for all job records.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    -- Identity
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Job definition
    job_type        VARCHAR(128) NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    priority        SMALLINT NOT NULL DEFAULT 5
                    CHECK (priority >= 0 AND priority <= 9),
    
    -- Execution control
    status          job_status NOT NULL DEFAULT 'pending',
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3
                    CHECK (max_retries >= 0 AND max_retries <= 20),
    timeout_ms      INTEGER NOT NULL DEFAULT 30000
                    CHECK (timeout_ms > 0 AND timeout_ms <= 3600000),
    
    -- Cron support (NULL for one-time jobs)
    cron_expression VARCHAR(64),
    cron_next_run   TIMESTAMPTZ,
    
    -- Execution metadata
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    worker_id       VARCHAR(64),
    error_message   TEXT,
    
    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT chk_cron_valid 
        CHECK (
            (cron_expression IS NULL AND cron_next_run IS NULL) OR
            (cron_expression IS NOT NULL AND cron_next_run IS NOT NULL)
        )
);

-- ---------------------------------------------------------------------------
-- Indexes on jobs table
-- ---------------------------------------------------------------------------

-- Primary queue polling index: pending jobs ordered by priority and schedule time
CREATE INDEX IF NOT EXISTS idx_jobs_poll 
    ON jobs (status, priority ASC, scheduled_at ASC) 
    WHERE status = 'pending';

-- Status-based lookup for monitoring and API queries
CREATE INDEX IF NOT EXISTS idx_jobs_status_created 
    ON jobs (status, created_at DESC);

-- Job type lookup for analytics
CREATE INDEX IF NOT EXISTS idx_jobs_type_status 
    ON jobs (job_type, status);

-- Worker assignment lookup
CREATE INDEX IF NOT EXISTS idx_jobs_worker_running 
    ON jobs (worker_id, status) 
    WHERE status = 'running';

-- Cron job scheduling index
CREATE INDEX IF NOT EXISTS idx_jobs_cron_next_run 
    ON jobs (cron_next_run) 
    WHERE cron_expression IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Table: job_history
-- Audit trail of all job state transitions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_history (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    old_status      job_status,
    new_status      job_status NOT NULL,
    worker_id       VARCHAR(64),
    error_message   TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Indexes on job_history
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_history_job_id 
    ON job_history (job_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_history_created 
    ON job_history (created_at DESC);

-- ---------------------------------------------------------------------------
-- Table: worker_heartbeats
-- For tracking active workers and detecting stale workers.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id       VARCHAR(64) PRIMARY KEY,
    hostname        VARCHAR(256) NOT NULL,
    concurrency     INTEGER NOT NULL DEFAULT 10,
    active_jobs     INTEGER NOT NULL DEFAULT 0,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_heartbeats_last 
    ON worker_heartbeats (last_heartbeat DESC);

-- ---------------------------------------------------------------------------
-- Function: Automatically update updated_at column
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Function: Log job status changes to history table
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION log_job_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO job_history (
            job_id, old_status, new_status, 
            worker_id, error_message, metadata
        ) VALUES (
            NEW.id, OLD.status, NEW.status,
            NEW.worker_id, NEW.error_message,
            jsonb_build_object(
                'retry_count', NEW.retry_count,
                'scheduled_at', NEW.scheduled_at
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_history
    AFTER UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION log_job_status_change();

-- ---------------------------------------------------------------------------
-- Views for monitoring
-- ---------------------------------------------------------------------------

-- Job status summary
CREATE OR REPLACE VIEW v_job_status_summary AS
SELECT 
    status,
    job_type,
    COUNT(*) as count,
    MIN(created_at) as oldest_job,
    MAX(created_at) as newest_job
FROM jobs
GROUP BY status, job_type;

-- Queue depth by priority
CREATE OR REPLACE VIEW v_queue_depth AS
SELECT 
    priority,
    COUNT(*) as depth,
    MIN(scheduled_at) as oldest_scheduled
FROM jobs
WHERE status = 'pending'
GROUP BY priority;

COMMIT;