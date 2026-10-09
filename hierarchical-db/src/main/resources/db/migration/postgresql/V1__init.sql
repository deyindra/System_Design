-- Hierarchical database schema, one copy per shard. See docs/DESIGN.md §4.

-- One row per space (a Confluence space or a Jira project). Also the unit of move serialization.
CREATE TABLE spaces (
    tenant_id     uuid    NOT NULL,
    space_id      bigint  NOT NULL,
    key           text    NOT NULL,
    root_node_id  bigint  NOT NULL,
    tree_version  bigint  NOT NULL DEFAULT 0,      -- bumped by every move; part of the cache key
    PRIMARY KEY (tenant_id, space_id),
    UNIQUE (tenant_id, key)
);

-- The tree. Hot, narrow row: no body here.
CREATE TABLE nodes (
    tenant_id   uuid      NOT NULL,
    node_id     bigint    NOT NULL,                -- 64-bit Snowflake id (time | worker | seq), globally unique
    space_id    bigint    NOT NULL,
    parent_id   bigint,                            -- NULL only for a space root
    path        text      COLLATE "C" NOT NULL,    -- '/100/104/101/' : ancestors then self, '/'-terminated
    depth       smallint  NOT NULL CHECK (depth BETWEEN 0 AND 63),
    rank        text      COLLATE "C" NOT NULL,    -- LexoRank sibling order
    node_type   text      NOT NULL,                -- 'space', 'page', 'folder', 'epic', 'story', 'subtask'
    title       text      NOT NULL,
    status      text      NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','TRASHED')),
    trashed_at  timestamptz,
    version     int       NOT NULL DEFAULT 1,      -- optimistic concurrency for edits
    created_by  text      NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, node_id),
    FOREIGN KEY (tenant_id, parent_id) REFERENCES nodes (tenant_id, node_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (path LIKE '%/' || node_id || '/')        -- the path always ends with self
) WITH (fillfactor = 80);                          -- room for HOT updates of title/version/rank

-- Children in sibling order, keyset-paged. node_id breaks rank ties from concurrent inserts.
CREATE INDEX ix_nodes_children ON nodes (tenant_id, parent_id, rank, node_id);
-- Subtree = prefix range scan. COLLATE "C" makes byte order = prefix order.
CREATE INDEX ix_nodes_path     ON nodes (tenant_id, path);
-- The (small) set of trashed roots per space, used to hide trashed subtrees from subtree reads.
CREATE INDEX ix_nodes_trashed  ON nodes (tenant_id, space_id) WHERE status = 'TRASHED';

CREATE TABLE node_content (
    tenant_id    uuid   NOT NULL,
    node_id      bigint NOT NULL,
    body         text   NOT NULL,                  -- or a pointer to object storage for large bodies
    body_version int    NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, node_id)
);

-- Inherited restrictions: a row on an ancestor applies to every descendant.
CREATE TABLE restrictions (
    tenant_id  uuid   NOT NULL,
    node_id    bigint NOT NULL,
    op         text   NOT NULL CHECK (op IN ('view','edit')),
    principal  text   NOT NULL,                     -- 'user:42' or 'group:eng'
    PRIMARY KEY (tenant_id, node_id, op, principal)
);

-- Path-rewrite overlay for large moves (§8.5). At most one RUNNING job per space.
CREATE TABLE move_jobs (
    tenant_id    uuid     NOT NULL,
    job_id       bigint   NOT NULL,
    space_id     bigint   NOT NULL,
    root_node_id bigint   NOT NULL,
    old_prefix   text     COLLATE "C" NOT NULL,
    new_prefix   text     COLLATE "C" NOT NULL,
    depth_delta  smallint NOT NULL,
    state        text     NOT NULL CHECK (state IN ('RUNNING','DONE')),
    rows_done    bigint   NOT NULL DEFAULT 0,
    lease_owner  text,                              -- the worker currently rewriting batches
    lease_until  timestamptz,                       -- another worker may take over once this passes
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    PRIMARY KEY (tenant_id, job_id)
);
CREATE UNIQUE INDEX ux_move_jobs_one_running ON move_jobs (tenant_id, space_id) WHERE state = 'RUNNING';

-- Transactional outbox: written in the same transaction as the change, then relayed to Kafka.
CREATE TABLE outbox (
    seq        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id  uuid   NOT NULL,
    event_type text   NOT NULL,                     -- NODE_CREATED, NODE_MOVED, NODE_UPDATED, NODE_TRASHED, ...
    payload    jsonb  NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- One row. The relay holds it with FOR UPDATE SKIP LOCKED, so one pod relays a shard at a time.
CREATE TABLE outbox_relay_state (
    id          int    PRIMARY KEY CHECK (id = 1),
    relayed_seq bigint NOT NULL DEFAULT 0
);
INSERT INTO outbox_relay_state (id) VALUES (1);

-- Create is claimed by its key in the same transaction, so a retry either waits for the first attempt or replays it.
CREATE TABLE idempotency_keys (
    tenant_id    uuid   NOT NULL,
    key          text   NOT NULL,
    request_hash text   NOT NULL,                   -- a different body under the same key is rejected (422)
    node_id      bigint NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);
