-- Jobs: few rows, transactional, queried by tenant. The crawl graph lives in Cassandra.
CREATE TABLE IF NOT EXISTS crawl_jobs (
    job_id          text        PRIMARY KEY,
    tenant_id       text        NOT NULL,
    idempotency_key text,
    request         jsonb       NOT NULL,
    status          text        NOT NULL,
    created_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    error           text,
    stats           jsonb,      -- frozen when the job becomes terminal; live counters are in Redis until then
    UNIQUE (tenant_id, idempotency_key)  -- NULL keys never conflict: no key, no idempotency
);
CREATE INDEX IF NOT EXISTS crawl_jobs_by_tenant ON crawl_jobs (tenant_id, created_at DESC);
