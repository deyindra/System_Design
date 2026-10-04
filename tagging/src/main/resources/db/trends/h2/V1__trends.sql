-- Trending projection for H2 (local profile and tests). Same tables and keys as the PostgreSQL migration.

CREATE TABLE tag_activity (
    tenant_id VARCHAR(64) NOT NULL,
    grain     INT         NOT NULL,
    bucket    BIGINT      NOT NULL,
    tag_id    BIGINT      NOT NULL,
    attaches  BIGINT      NOT NULL,
    CONSTRAINT pk_tag_activity PRIMARY KEY (tenant_id, grain, bucket, tag_id)
);
CREATE INDEX ix_tag_activity_prune ON tag_activity (grain, bucket);

CREATE TABLE trend_progress (
    tenant_id    VARCHAR(64) NOT NULL,
    source_shard VARCHAR(64) NOT NULL,
    last_seq     BIGINT      NOT NULL,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_trend_progress PRIMARY KEY (tenant_id, source_shard)
);
