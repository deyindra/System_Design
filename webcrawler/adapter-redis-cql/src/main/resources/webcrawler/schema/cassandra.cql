-- %1$s is the keyspace. Statements are separated by ';' at the end of a line.

-- One row per node of one job's graph. Its own partition: always read by key.
-- depth and parent_hash are written USING TIMESTAMP derived from the depth (smaller depth = larger timestamp), so
-- last-write-wins keeps the shortest path whatever order concurrent writers land in.
CREATE TABLE IF NOT EXISTS %1$s.job_pages (
    job_id       text,
    url_hash     text,
    url          text,
    type         text,
    depth        int,
    parent_hash  text,
    status       text,
    http_status  int,
    content_type text,
    content_hash text,
    size         bigint,
    fetched_at   timestamp,
    duplicate_of text,
    redirect_to  text,
    error        text,
    PRIMARY KEY ((job_id, url_hash))
);

-- Discovery (BFS) order, for listing: seq comes from Redis; chunks of 10 000 bound the partition size.
CREATE TABLE IF NOT EXISTS %1$s.job_pages_by_order (
    job_id   text,
    chunk    bigint,
    seq      bigint,
    url_hash text,
    PRIMARY KEY ((job_id, chunk), seq)
);

-- Edges. Two spellings of one target are two rows: the link rewriter needs each raw href.
CREATE TABLE IF NOT EXISTS %1$s.page_links (
    job_id    text,
    from_hash text,
    to_hash   text,
    raw_href  text,
    to_url    text,
    type      text,
    PRIMARY KEY ((job_id, from_hash), to_hash, raw_href)
);

-- "Have we crawled this?" per tenant: the job whose node has the newest content (written USING TIMESTAMP fetched_at).
CREATE TABLE IF NOT EXISTS %1$s.tenant_pages (
    tenant_id text,
    url_hash  text,
    job_id    text,
    PRIMARY KEY ((tenant_id, url_hash))
);
