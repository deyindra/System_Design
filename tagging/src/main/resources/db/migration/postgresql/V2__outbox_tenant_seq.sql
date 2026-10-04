-- Read barrier for STRONG search: MAX(seq) of one tenant's outbox rows, answered from this index alone.
CREATE INDEX ix_outbox_tenant_seq ON outbox (tenant_id, seq);
