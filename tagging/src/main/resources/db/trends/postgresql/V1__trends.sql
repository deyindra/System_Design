-- Trending projection: time-bucketed attach counts per (tenant, tag), built from the tag-events topic.
-- Derived data: it lives in its own database and can be rebuilt by replaying the topic.

-- One row per (tenant, grain, bucket, tag) with activity. grain is the bucket width in seconds
-- (3600 = hourly for the 24 h window, 86400 = daily for the 7 d window); bucket = epoch_seconds / grain.
-- A window query is one PK range scan: tenant_id, grain, bucket BETWEEN a AND b.
CREATE TABLE tag_activity (
    tenant_id VARCHAR(64) COLLATE "C" NOT NULL,
    grain     INT         NOT NULL,
    bucket    BIGINT      NOT NULL,
    tag_id    BIGINT      NOT NULL,
    attaches  BIGINT      NOT NULL,
    CONSTRAINT pk_tag_activity PRIMARY KEY (tenant_id, grain, bucket, tag_id)
) WITH (fillfactor = 80);   -- the current bucket's rows are updated in place (HOT)
-- Retention: DELETE ... WHERE grain = ? AND bucket < ?. At scale, range-partition by bucket and drop partitions.
CREATE INDEX ix_tag_activity_prune ON tag_activity (grain, bucket);

-- Exactly-once counting: the highest seq applied per (tenant, source shard), updated in the same
-- transaction as the counts. A redelivered event has seq <= last_seq and is skipped. Keyed by the
-- source shard because seq is per shard: a tenant moved to another shard starts a new sequence.
CREATE TABLE trend_progress (
    tenant_id    VARCHAR(64) COLLATE "C" NOT NULL,
    source_shard VARCHAR(64) NOT NULL,
    last_seq     BIGINT      NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_trend_progress PRIMARY KEY (tenant_id, source_shard)
);
