package com.salesforce.einstein.webcrawler.adapter.rediscql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.CrawlTask;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStats;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.store.Admission;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.store.Slice;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * The {@link JobStore} on three stores, each doing what it is good at:
 * <ul>
 *   <li><b>Redis</b>: the hot, contended per-job state, changed by Lua scripts so each decision is atomic: the
 *       seen-set with each node's current depth, budgets, {@code pending}, task keys, content owners, scope keys,
 *       trap counters and exact per-status counters. Every key of a job contains {@code {jobId}} in braces, so a
 *       job's keys share a Redis Cluster slot and a script may touch all of them.</li>
 *   <li><b>Cassandra</b>: the crawl graph, which is large and append-mostly: {@code job_pages},
 *       {@code job_pages_by_order}, {@code page_links}, {@code tenant_pages}.</li>
 *   <li><b>Postgres</b>: {@code crawl_jobs}, the few rows that need a unique constraint (idempotency keys) and a
 *       compare-and-set (status).</li>
 * </ul>
 *
 * <p>Depth: Redis is the authority while a job runs, and only the admit script lowers it. That is what makes the
 * engine's relaxation sound across nodes: a discoverer lowers the depth (Redis) then reads the status (Cassandra); a
 * worker writes the status then reads the depth, so one of them sees the other's write. Cassandra's copy is written
 * with a timestamp derived from the depth (smaller depth, larger timestamp), so last-write-wins converges on the
 * minimum whatever order the writes land in; it is what remains once a finished job's Redis keys expire.
 *
 * <p>Statements are idempotent, so a replayed task rewriting a row is harmless.
 */
public final class RedisCqlJobStore implements JobStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedisCqlJobStore.class);

    /** Rows of {@code job_pages_by_order} per partition. */
    static final long CHUNK = 10_000;
    private static final String DONE_CHANNEL = "job-done:";

    private final RedisCqlSettings settings;
    private final CqlSession cql;
    private final DataSource db;
    private volatile AutoCloseable ownedPool;                 // set by open(): the pool this store created
    private final StatefulRedisConnection<String, String> conn;
    private final RedisCommands<String, String> redis;
    private final StatefulRedisPubSubConnection<String, String> events;
    private final ExecutorService notifier = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "job-done-listener");
        t.setDaemon(true);
        return t;
    });
    private final List<Consumer<CrawlJob>> listeners = new CopyOnWriteArrayList<>();
    private final ObjectMapper json = JsonMapper.builder().findAndAddModules()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private record Cached(CrawlJob job, long at) { }
    private final Map<String, Cached> jobCache = new ConcurrentHashMap<>();
    private final Map<String, JobStats> frozen = new ConcurrentHashMap<>();

    private final Script admit = Script.load("admit");
    private final Script openTask = Script.load("open_task");
    private final Script closeTask = Script.load("close_task");
    private final Script nodeState = Script.load("node_state");

    private final PreparedStatement upsertPage, upsertDepth, selectPage, insertOrder, selectOrder,
            insertLink, selectLinks, upsertTenant, selectTenant;

    /** With its own JDBC pool from the settings, closed with the store. */
    public static RedisCqlJobStore open(RedisCqlSettings settings, RedisClient redisClient, CqlSession cql) {
        var pool = RedisCqlSchema.dataSource(settings);
        try {
            RedisCqlJobStore store = new RedisCqlJobStore(settings, redisClient, cql, pool);
            store.ownedPool = pool;
            return store;
        } catch (RuntimeException e) {
            pool.close();
            throw e;
        }
    }

    public RedisCqlJobStore(RedisCqlSettings settings, RedisClient redisClient, CqlSession cql, DataSource db) {
        this.settings = settings;
        this.cql = cql;
        this.db = db;
        if (settings.createSchema()) RedisCqlSchema.create(cql, settings.keyspace(), settings.replication(), db);
        String ks = settings.keyspace();
        upsertPage = cql.prepare("UPDATE " + ks + ".job_pages SET url = ?, type = ?, status = ?, http_status = ?, "
                + "content_type = ?, content_hash = ?, size = ?, fetched_at = ?, duplicate_of = ?, redirect_to = ?, "
                + "error = ? WHERE job_id = ? AND url_hash = ?");
        upsertDepth = cql.prepare("UPDATE " + ks + ".job_pages SET depth = ?, parent_hash = ? WHERE job_id = ? AND url_hash = ?");
        selectPage = cql.prepare("SELECT * FROM " + ks + ".job_pages WHERE job_id = ? AND url_hash = ?");
        insertOrder = cql.prepare("INSERT INTO " + ks + ".job_pages_by_order (job_id, chunk, seq, url_hash) VALUES (?, ?, ?, ?)");
        selectOrder = cql.prepare("SELECT seq, url_hash FROM " + ks + ".job_pages_by_order "
                + "WHERE job_id = ? AND chunk = ? AND seq >= ? LIMIT ?");
        insertLink = cql.prepare("INSERT INTO " + ks + ".page_links (job_id, from_hash, to_hash, raw_href, to_url, type) "
                + "VALUES (?, ?, ?, ?, ?, ?)");
        selectLinks = cql.prepare("SELECT * FROM " + ks + ".page_links WHERE job_id = ? AND from_hash = ?");
        upsertTenant = cql.prepare("UPDATE " + ks + ".tenant_pages SET job_id = ? WHERE tenant_id = ? AND url_hash = ?");
        selectTenant = cql.prepare("SELECT job_id FROM " + ks + ".tenant_pages WHERE tenant_id = ? AND url_hash = ?");

        this.conn = redisClient.connect();
        this.redis = conn.sync();
        for (Script s : List.of(admit, openTask, closeTask, nodeState)) s.sha = redis.scriptLoad(s.body);

        // Every node hears every terminal transition, whoever made it: that is how a sync caller here is woken by
        // workers elsewhere. Subscribed before the constructor returns, so no event after it can be missed.
        this.events = redisClient.connectPubSub();
        events.addListener(new RedisPubSubAdapter<>() {
            @Override public void message(String pattern, String channel, String message) {
                notifier.execute(() -> terminalEvent(channel.substring(DONE_CHANNEL.length())));
            }
        });
        events.sync().psubscribe(DONE_CHANNEL + "*");
    }

    // ------------------------------------------------------------------ keys

    /** {@code wc:{job}:name}: the braces put all of a job's keys in one cluster slot. */
    private static String key(String jobId, String name) { return "wc:{" + jobId + "}:" + name; }

    private static String[] keys(String jobId, String... names) {
        String[] out = new String[names.length];
        for (int i = 0; i < names.length; i++) out[i] = key(jobId, names[i]);
        return out;
    }

    private static final String[] HOT = {"nodes", "ctr", "tasks", "state", "content", "scope", "variants"};

    // ------------------------------------------------------------------ jobs (Postgres)

    @Override public CrawlJob createOrGet(CrawlJob job) {
        String insert = "INSERT INTO crawl_jobs (job_id, tenant_id, idempotency_key, request, status, created_at) "
                + "VALUES (?, ?, ?, ?::jsonb, ?, ?) ON CONFLICT (tenant_id, idempotency_key) DO NOTHING";
        try (Connection c = db.getConnection(); java.sql.PreparedStatement ps = c.prepareStatement(insert)) {
            ps.setString(1, job.jobId());
            ps.setString(2, job.tenantId());
            ps.setString(3, job.idempotencyKey());
            ps.setString(4, json.writeValueAsString(job.request()));
            ps.setString(5, job.status().name());
            ps.setTimestamp(6, Timestamp.from(job.createdAt()));
            if (ps.executeUpdate() == 1) return job;
        } catch (SQLException | IOException e) {
            throw failure("createOrGet", e);
        }
        return query("SELECT * FROM crawl_jobs WHERE tenant_id = ? AND idempotency_key = ?",
                job.tenantId(), job.idempotencyKey()).orElseThrow();
    }

    /**
     * Cached briefly: the engine reads the job for every task (was it canceled?). A terminal job never changes,
     * so it stays cached; a running one is read again after {@code jobCacheTtl}, or at once on a transition here or a
     * {@code job-done} event from anywhere.
     */
    @Override public Optional<CrawlJob> get(String jobId) {
        Cached c = jobCache.get(jobId);
        long now = System.currentTimeMillis();
        if (c != null && (c.job.status().isTerminal() || now - c.at < settings.jobCacheTtl().toMillis()))
            return Optional.of(c.job);
        Optional<CrawlJob> j = query("SELECT * FROM crawl_jobs WHERE job_id = ?", jobId);
        j.ifPresent(x -> jobCache.put(jobId, new Cached(x, now)));
        return j;
    }

    @Override public boolean transition(String jobId, JobStatus from, JobStatus to, String error) {
        boolean terminal = to.isTerminal();
        String sql = "UPDATE crawl_jobs SET status = ?, error = ?, finished_at = COALESCE(?, finished_at), "
                + "stats = COALESCE(?::jsonb, stats) WHERE job_id = ? AND status = ?";
        int n;
        try (Connection c = db.getConnection(); java.sql.PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, to.name());
            ps.setString(2, error);
            ps.setTimestamp(3, terminal ? Timestamp.from(Instant.now()) : null);
            ps.setString(4, terminal ? json.writeValueAsString(liveStats(jobId)) : null);   // frozen with the status
            ps.setString(5, jobId);
            ps.setString(6, from.name());
            n = ps.executeUpdate();
        } catch (SQLException | IOException e) {
            throw failure("transition", e);
        }
        jobCache.remove(jobId);
        if (n == 0) return false;
        if (terminal) {
            redis.publish(DONE_CHANNEL + jobId, to.name());
            for (String k : keys(jobId, HOT)) redis.expire(k, settings.hotStateTtl().toSeconds());
        }
        return true;
    }

    @Override public void onTerminal(Consumer<CrawlJob> listener) { listeners.add(listener); }

    private void terminalEvent(String jobId) {
        jobCache.remove(jobId);
        get(jobId).filter(j -> j.status().isTerminal()).ifPresent(j -> {
            for (Consumer<CrawlJob> l : listeners) {
                try { l.accept(j); } catch (RuntimeException e) { log.warn("terminal listener failed", e); }
            }
        });
    }

    private Optional<CrawlJob> query(String sql, String... args) {
        try (Connection c = db.getConnection(); java.sql.PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setString(i + 1, args[i]);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                Timestamp fin = rs.getTimestamp("finished_at");
                return Optional.of(new CrawlJob(rs.getString("job_id"), rs.getString("tenant_id"),
                        rs.getString("idempotency_key"), json.readValue(rs.getString("request"), CrawlRequest.class),
                        JobStatus.valueOf(rs.getString("status")), rs.getTimestamp("created_at").toInstant(),
                        fin == null ? null : fin.toInstant(), rs.getString("error")));
            }
        } catch (SQLException | IOException e) {
            throw failure("read job", e);
        }
    }

    // ------------------------------------------------------------------ the seen-test (Redis, then Cassandra)

    @Override public Admission admit(JobPage page, boolean enqueue, long budget) {
        String taskKey = enqueue ? CrawlTask.key(page.urlHash(), page.depth(), 0) : "";
        List<Object> r = run(admit, ScriptOutputType.MULTI, keys(page.jobId(), "nodes", "ctr", "tasks"),
                page.urlHash(), Integer.toString(page.depth()), page.type().isAsset() ? "1" : "0",
                enqueue ? "1" : "0", Long.toString(budget), taskKey);
        Admission a = Admission.valueOf((String) r.get(0));
        long seq = (Long) r.get(1);
        switch (a) {
            case ADMITTED -> {
                // Written before the caller pushes the task, so a worker always finds the row.
                List<CompletionStage<AsyncResultSet>> writes = new ArrayList<>();
                writes.add(cql.executeAsync(pageRow(page)));
                writes.add(cql.executeAsync(depthCell(page, !enqueue)));
                if (seq > 0) writes.add(cql.executeAsync(insertOrder.bind(page.jobId(), (seq - 1) / CHUNK, seq, page.urlHash())));
                await(writes);
            }
            case SHALLOWER -> await(List.of(cql.executeAsync(depthCell(page, false))));
            default -> { }
        }
        return a;
    }

    @Override public void update(JobPage page) {
        List<CompletionStage<AsyncResultSet>> writes = new ArrayList<>();
        writes.add(cql.executeAsync(pageRow(page)));
        writes.add(cql.executeAsync(depthCell(page, page.status() == PageStatus.REFERENCED)));   // can't raise it
        if (page.status().hasContent() && page.fetchedAt() != null) {
            String tenant = get(page.jobId()).orElseThrow().tenantId();
            // newest content wins, whatever order the writes arrive in
            writes.add(cql.executeAsync(upsertTenant.bind(page.jobId(), tenant, page.urlHash())
                    .setQueryTimestamp(ChronoUnit.MICROS.between(Instant.EPOCH, page.fetchedAt()))));
        }
        run(nodeState, ScriptOutputType.INTEGER, keys(page.jobId(), "state", "ctr"), page.urlHash(), counters(page));
        await(writes);
    }

    /** The exact counters a node in this state contributes to; {@link JobStats} is their sum. */
    private static String counters(JobPage p) {
        List<String> c = new ArrayList<>(2);
        if (p.status().hasContent()) c.add(p.type().isAsset() ? "assets" : "pages");
        if (p.status() == PageStatus.REUSED) c.add("reused");
        if (p.status() == PageStatus.DUPLICATE) c.add("duplicates");
        if (p.status() == PageStatus.FAILED || p.status() == PageStatus.TOO_LARGE) c.add("failed");
        return String.join(",", c);
    }

    /** Every column but depth and parent, timestamped when written. */
    private BoundStatement pageRow(JobPage p) {
        return upsertPage.bind(p.url(), p.type().name(), p.status().name(), p.httpStatus(), p.contentType(),
                p.contentHash(), p.size(), p.fetchedAt(), p.duplicateOf(), p.redirectTo(), p.error(),
                p.jobId(), p.urlHash());
    }

    /**
     * Depth and parent, timestamped so that the smallest depth wins. A referenced-only node ranks below every
     * queued one: when an embed becomes a crawled page, the page's depth replaces the embed's.
     */
    private BoundStatement depthCell(JobPage p, boolean referenced) {
        long ts = (referenced ? 1_000_000L : 2_000_000L) - p.depth();
        return upsertDepth.bind(p.depth(), p.parentHash(), p.jobId(), p.urlHash()).setQueryTimestamp(ts);
    }

    @Override public Optional<JobPage> page(String jobId, String urlHash) {
        CompletionStage<AsyncResultSet> row = cql.executeAsync(selectPage.bind(jobId, urlHash));
        String node = redis.hget(key(jobId, "nodes"), urlHash);
        Row r = await(row).one();
        if (r == null || r.isNull("status")) return Optional.empty();       // admitted, row not written yet
        int depth = node != null ? Integer.parseInt(node.substring(0, node.indexOf('|'))) : r.getInt("depth");
        return Optional.of(new JobPage(jobId, urlHash, r.getString("url"), ResourceType.valueOf(r.getString("type")),
                depth, r.getString("parent_hash"), PageStatus.valueOf(r.getString("status")), r.getInt("http_status"),
                r.getString("content_type"), r.getString("content_hash"), r.getLong("size"), r.getInstant("fetched_at"),
                r.getString("duplicate_of"), r.getString("redirect_to"), r.getString("error")));
    }

    @Override public Optional<JobPage> latestForTenant(String tenantId, String urlHash) {
        Row r = cql.execute(selectTenant.bind(tenantId, urlHash)).one();
        return r == null ? Optional.empty() : page(r.getString("job_id"), urlHash);
    }

    // ------------------------------------------------------------------ hot state (Redis)

    @Override public Optional<String> claimContent(String jobId, String contentKey, String urlHash) {
        String k = key(jobId, "content");
        if (redis.hsetnx(k, contentKey, urlHash)) return Optional.empty();
        String owner = redis.hget(k, contentKey);                 // set once, never changed: no race with the above
        return owner == null || owner.equals(urlHash) ? Optional.empty() : Optional.of(owner);
    }

    @Override public boolean openTask(String jobId, String taskKey) {
        Long r = run(openTask, ScriptOutputType.INTEGER, keys(jobId, "tasks", "ctr"), taskKey);
        return r == 1;
    }

    @Override public boolean isTaskOpen(String jobId, String taskKey) {
        return "1".equals(redis.hget(key(jobId, "tasks"), taskKey));
    }

    @Override public OptionalLong closeTask(String jobId, String taskKey) {
        List<Long> r = run(closeTask, ScriptOutputType.MULTI, keys(jobId, "tasks", "ctr"), taskKey);
        return r.get(0) == 1 ? OptionalLong.of(r.get(1)) : OptionalLong.empty();
    }

    @Override public void addScopeKey(String jobId, String k) { redis.sadd(key(jobId, "scope"), k); }

    @Override public boolean hasScopeKey(String jobId, String k) { return redis.sismember(key(jobId, "scope"), k); }

    @Override public long countVariant(String jobId, String pathKey) { return redis.hincrby(key(jobId, "variants"), pathKey, 1); }

    @Override public void incrementPending(String jobId) { redis.hincrby(key(jobId, "ctr"), "pending", 1); }

    @Override public long decrementPending(String jobId) { return redis.hincrby(key(jobId, "ctr"), "pending", -1); }

    @Override public void markTruncated(String jobId) { redis.hincrby(key(jobId, "ctr"), "truncated", 1); }

    @Override public JobStats stats(String jobId) {
        JobStats f = frozen.get(jobId);
        if (f != null) return f;
        CrawlJob job = get(jobId).orElseThrow(() -> new IllegalArgumentException("unknown job " + jobId));
        if (job.status().isTerminal()) {
            Optional<JobStats> s = frozenStats(jobId);
            if (s.isPresent()) {
                frozen.put(jobId, s.get());
                return s.get();
            }
        }
        return liveStats(jobId);
    }

    private JobStats liveStats(String jobId) {
        long discovered = redis.hlen(key(jobId, "nodes"));
        List<KeyValue<String, String>> v = redis.hmget(key(jobId, "ctr"),
                "pending", "pages", "assets", "reused", "duplicates", "failed", "edges", "truncated");
        long[] n = new long[v.size()];
        for (int i = 0; i < n.length; i++) n[i] = v.get(i).hasValue() ? Long.parseLong(v.get(i).getValue()) : 0;
        return new JobStats(discovered, n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7]);
    }

    private Optional<JobStats> frozenStats(String jobId) {
        try (Connection c = db.getConnection();
             java.sql.PreparedStatement ps = c.prepareStatement("SELECT stats FROM crawl_jobs WHERE job_id = ?")) {
            ps.setString(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || rs.getString(1) == null) return Optional.empty();
                return Optional.of(json.readValue(rs.getString(1), JobStats.class));
            }
        } catch (SQLException | IOException e) {
            throw failure("read stats", e);
        }
    }

    // ------------------------------------------------------------------ graph (Cassandra)

    @Override public void addLink(LinkEdge e) {
        cql.execute(insertLink.bind(e.jobId(), e.fromHash(), e.toHash(), e.rawHref(), e.toUrl(), e.type().name()));
        redis.hincrby(key(e.jobId(), "ctr"), "edges", 1);
    }

    @Override public List<LinkEdge> outLinks(String jobId, String fromHash) {
        List<LinkEdge> out = new ArrayList<>();
        for (Row r : cql.execute(selectLinks.bind(jobId, fromHash)))
            out.add(new LinkEdge(jobId, fromHash, r.getString("to_hash"), r.getString("to_url"),
                    ResourceType.valueOf(r.getString("type")), r.getString("raw_href")));
        return out;
    }

    /**
     * The cursor is the next discovery sequence number. Sequence numbers are dense (one per node, from the admit
     * script), so the scan walks chunks up to the job's last one and skips nothing but a crash's rare gap.
     */
    @Override public Slice<JobPage> pages(String jobId, ResourceType type, String cursor, int limit) {
        String last = redis.hget(key(jobId, "ctr"), "seq");
        long max = last != null ? Long.parseLong(last) : stats(jobId).discovered();
        long next = cursor == null ? 1 : Long.parseLong(cursor);
        List<JobPage> items = new ArrayList<>();
        while (next <= max && items.size() < limit) {
            long chunk = (next - 1) / CHUNK;
            long resume = (chunk + 1) * CHUNK + 1;                   // first seq of the next chunk
            for (Row r : cql.execute(selectOrder.bind(jobId, chunk, next, (int) Math.min(CHUNK, 1000)))) {
                long seq = r.getLong("seq");
                resume = seq + 1;
                Optional<JobPage> p = page(jobId, r.getString("url_hash"));
                if (p.isPresent() && (type == null || p.get().type() == type)) items.add(p.get());
                if (items.size() == limit) break;
            }
            next = resume;
        }
        return new Slice<>(items, next <= max ? Long.toString(next) : null);
    }

    // ------------------------------------------------------------------ plumbing

    /** A Lua script, run by digest; reloaded if Redis has forgotten it (a restart or SCRIPT FLUSH). */
    private static final class Script {
        final String body;
        volatile String sha;
        private Script(String body) { this.body = body; }

        static Script load(String name) {
            try (InputStream in = RedisCqlJobStore.class.getResourceAsStream("/webcrawler/lua/" + name + ".lua")) {
                if (in == null) throw new IllegalStateException("missing script " + name);
                return new Script(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private <T> T run(Script s, ScriptOutputType type, String[] keys, String... args) {
        try {
            return redis.evalsha(s.sha, type, keys, args);
        } catch (RedisNoScriptException e) {
            s.sha = redis.scriptLoad(s.body);
            return redis.evalsha(s.sha, type, keys, args);
        }
    }

    private static AsyncResultSet await(CompletionStage<AsyncResultSet> f) {
        return f.toCompletableFuture().join();
    }

    private static void await(List<CompletionStage<AsyncResultSet>> fs) {
        for (CompletionStage<AsyncResultSet> f : fs) f.toCompletableFuture().join();
    }

    private static IllegalStateException failure(String what, Exception e) {
        return new IllegalStateException("job store: " + what + " failed", e);
    }

    @Override public void close() {
        events.close();
        conn.close();
        notifier.shutdownNow();
        if (ownedPool != null) {
            try { ownedPool.close(); } catch (Exception e) { log.warn("closing the job pool failed", e); }
        }
    }
}
