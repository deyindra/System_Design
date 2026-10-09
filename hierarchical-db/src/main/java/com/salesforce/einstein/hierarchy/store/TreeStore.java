package com.salesforce.einstein.hierarchy.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.hierarchy.domain.Cursors;
import com.salesforce.einstein.hierarchy.domain.MoveJob;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.NodeStatus;
import com.salesforce.einstein.hierarchy.domain.Overlay;
import com.salesforce.einstein.hierarchy.domain.Paths;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Every SQL statement of the tree, PostgreSQL dialect. Stateless: each method runs on the {@link JdbcTemplate} of the
 * transaction the caller opened ({@link Db#write} or {@link Db#read}). Node rows come back as stored; the service
 * applies the {@link Overlay}, except in {@link #descendants}, which must translate in SQL to order and page.
 */
public final class TreeStore {
    private static final String NODE_COLS = "node_id, space_id, parent_id, path, depth, rank, node_type, title, "
            + "status, version, created_by, created_at, updated_at";
    private static final RowMapper<Node> NODE = (rs, i) -> node(rs, "path", "depth");
    private static final RowMapper<MoveJob> JOB = (rs, i) -> new MoveJob(rs.getLong("job_id"), rs.getLong("space_id"),
            rs.getLong("root_node_id"), rs.getString("old_prefix"), rs.getString("new_prefix"),
            rs.getInt("depth_delta"), MoveJob.State.valueOf(rs.getString("state")), rs.getLong("rows_done"),
            rs.getTimestamp("created_at").toInstant(), instant(rs.getTimestamp("finished_at")));
    private static final String JOB_COLS = "job_id, space_id, root_node_id, old_prefix, new_prefix, depth_delta, "
            + "state, rows_done, created_at, finished_at";

    private final ObjectMapper json;

    public TreeStore(ObjectMapper json) {
        this.json = json;
    }

    // ---------------------------------------------------------------- spaces

    public void insertSpace(JdbcTemplate j, UUID t, Space s) {
        j.update("INSERT INTO spaces (tenant_id, space_id, key, root_node_id, tree_version) VALUES (?, ?, ?, ?, ?)",
                t, s.id(), s.key(), s.rootNodeId(), s.treeVersion());
    }

    public Optional<Space> space(JdbcTemplate j, UUID t, long spaceId) {
        return j.query("SELECT space_id, key, root_node_id, tree_version FROM spaces WHERE tenant_id = ? AND space_id = ?",
                (rs, i) -> new Space(rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4)), t, spaceId)
                .stream().findFirst();
    }

    public long treeVersion(JdbcTemplate j, UUID t, long spaceId) {
        return j.query("SELECT tree_version FROM spaces WHERE tenant_id = ? AND space_id = ?",
                (rs, i) -> rs.getLong(1), t, spaceId).stream().findFirst().orElse(0L);
    }

    /** Every cached structure of the space (breadcrumbs, children pages) is keyed by this, so this retires them all. */
    public long bumpTreeVersion(JdbcTemplate j, UUID t, long spaceId) {
        return Objects.requireNonNull(j.queryForObject("UPDATE spaces SET tree_version = tree_version + 1 "
                + "WHERE tenant_id = ? AND space_id = ? RETURNING tree_version", Long.class, t, spaceId));
    }

    // ---------------------------------------------------------------- nodes: point reads

    public Optional<Node> node(JdbcTemplate j, UUID t, long id) {
        return j.query("SELECT " + NODE_COLS + " FROM nodes WHERE tenant_id = ? AND node_id = ?", NODE, t, id)
                .stream().findFirst();
    }

    /** Breadcrumbs: the path lists the ancestor ids, so this is one round trip of primary-key probes, no recursion. */
    public List<Node> nodes(JdbcTemplate j, UUID t, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return j.query("SELECT " + NODE_COLS + " FROM nodes WHERE tenant_id = ? AND node_id = ANY (?::bigint[])",
                NODE, t, idArray(ids));
    }

    /** True if any of {@code ids} (normally a path) is TRASHED: then the node is in the trash. */
    public boolean anyTrashed(JdbcTemplate j, UUID t, Collection<Long> ids) {
        return !j.query("SELECT 1 FROM nodes WHERE tenant_id = ? AND node_id = ANY (?::bigint[]) "
                + "AND status = 'TRASHED' LIMIT 1", (rs, i) -> 1, t, idArray(ids)).isEmpty();
    }

    @Nullable
    public String body(JdbcTemplate j, UUID t, long id) {
        return j.query("SELECT body FROM node_content WHERE tenant_id = ? AND node_id = ?",
                (rs, i) -> rs.getString(1), t, id).stream().findFirst().orElse(null);
    }

    // ---------------------------------------------------------------- nodes: lists

    /** One keyset page of ACTIVE children in sibling order; an index-only range on {@code ix_nodes_children}. */
    public List<Node> children(JdbcTemplate j, UUID t, long parentId, @Nullable Cursors.ChildKey after, int limit) {
        if (after == null) {
            return j.query("SELECT " + NODE_COLS + " FROM nodes WHERE tenant_id = ? AND parent_id = ? "
                    + "AND status = 'ACTIVE' ORDER BY rank, node_id LIMIT ?", NODE, t, parentId, limit);
        }
        return j.query("SELECT " + NODE_COLS + " FROM nodes WHERE tenant_id = ? AND parent_id = ? "
                        + "AND status = 'ACTIVE' AND (rank, node_id) > (?, ?) ORDER BY rank, node_id LIMIT ?",
                NODE, t, parentId, after.rank(), after.nodeId(), limit);
    }

    /**
     * A subtree page in pre-order (path order), excluding the root itself, with effective paths.
     *
     * <p>Without an overlay this is one range scan of {@code ix_nodes_path}. With one, rows are read from the range
     * under {@code prefix} plus, if the moved subtree now lives under {@code prefix}, the range its un-migrated rows
     * are still stored at; each row's effective path is computed in SQL and filtered, ordered and paged on that.
     *
     * @param after    the last effective path returned so far ({@code prefix} on the first page)
     * @param maxDepth absolute depth limit
     * @param trashed  ids of the space's TRASHED nodes; rows below any of them are hidden
     */
    public List<Node> descendants(JdbcTemplate j, UUID t, String prefix, Overlay o, String after, int maxDepth,
                                  Collection<Long> trashed, int limit) {
        String trashFilter = trashed.isEmpty() ? ""
                : " AND NOT (string_to_array(trim(both '/' from %s), '/')::bigint[] && ?::bigint[])";
        List<Object> args = new ArrayList<>();
        args.add(t);
        if (!o.isActive()) {
            args.addAll(List.of(after, Paths.upperBound(prefix), maxDepth));
            if (!trashed.isEmpty()) {
                args.add(idArray(trashed));
            }
            args.add(limit);
            return j.query("SELECT " + NODE_COLS + " FROM nodes WHERE tenant_id = ? AND path > ? AND path < ? "
                    + "AND depth <= ?" + trashFilter.formatted("path") + " ORDER BY path LIMIT ?", NODE, args.toArray());
        }
        String also = storedRangeAlso(prefix, o);
        String sql = overlayDescendantsSql(also != null, trashFilter);
        args.clear();
        String from = o.oldPrefix();
        args.addAll(List.of(from, o.newPrefix(), from.length() + 1, from, o.depthDelta()));
        args.add(t);
        args.addAll(List.of(prefix, Paths.upperBound(prefix)));
        if (also != null) {
            args.addAll(List.of(also, Paths.upperBound(also)));
        }
        args.addAll(List.of(prefix, after, maxDepth));
        if (!trashed.isEmpty()) {
            args.add(idArray(trashed));
        }
        args.add(limit);
        return j.query(sql, (rs, i) -> node(rs, "effective_path", "effective_depth"), args.toArray());
    }

    /** Where un-migrated rows are stored that now (effectively) live under {@code prefix}; null if nowhere. */
    @Nullable
    private static String storedRangeAlso(String prefix, Overlay o) {
        if (prefix.startsWith(o.newPrefix())) {
            return o.oldPrefix() + prefix.substring(o.newPrefix().length());
        }
        return o.newPrefix().startsWith(prefix) ? o.oldPrefix() : null;
    }

    private static String overlayDescendantsSql(boolean twoRanges, String trashFilter) {
        String ranges = "(path >= ? AND path < ?)" + (twoRanges ? " OR (path >= ? AND path < ?)" : "");
        return "SELECT * FROM ("
                + " SELECT " + NODE_COLS + ","
                + "   CASE WHEN starts_with(path, ?) THEN ? || substr(path, ?) ELSE path END AS effective_path,"
                + "   depth + CASE WHEN starts_with(path, ?) THEN ? ELSE 0 END AS effective_depth"
                + " FROM nodes WHERE tenant_id = ? AND (" + ranges + ")"
                + ") x WHERE starts_with(effective_path, ?) AND effective_path > ? AND effective_depth <= ?"
                + trashFilter.formatted("effective_path") + " ORDER BY effective_path LIMIT ?";
    }

    public List<Long> trashedIds(JdbcTemplate j, UUID t, long spaceId) {
        return j.query("SELECT node_id FROM nodes WHERE tenant_id = ? AND space_id = ? AND status = 'TRASHED'",
                (rs, i) -> rs.getLong(1), t, spaceId);
    }

    public List<Node> trashedNodes(JdbcTemplate j, UUID t, long spaceId, int limit) {
        return j.query("SELECT " + NODE_COLS + " FROM nodes WHERE tenant_id = ? AND space_id = ? "
                + "AND status = 'TRASHED' ORDER BY trashed_at DESC, node_id LIMIT ?", NODE, t, spaceId, limit);
    }

    // ---------------------------------------------------------------- nodes: sibling ranks

    /** The highest rank under {@code parentId}, ignoring {@code exceptId} (the node being placed). */
    public Optional<String> maxRank(JdbcTemplate j, UUID t, long parentId, long exceptId) {
        return j.query("SELECT rank FROM nodes WHERE tenant_id = ? AND parent_id = ? AND node_id <> ? "
                + "ORDER BY rank DESC LIMIT 1", (rs, i) -> rs.getString(1), t, parentId, exceptId).stream().findFirst();
    }

    /** The lowest rank strictly above {@code rank}: the next distinct position after it. */
    public Optional<String> rankAbove(JdbcTemplate j, UUID t, long parentId, String rank, long exceptId) {
        return j.query("SELECT rank FROM nodes WHERE tenant_id = ? AND parent_id = ? AND rank > ? AND node_id <> ? "
                + "ORDER BY rank LIMIT 1", (rs, i) -> rs.getString(1), t, parentId, rank, exceptId)
                .stream().findFirst();
    }

    /** The highest rank strictly below {@code rank}. */
    public Optional<String> rankBelow(JdbcTemplate j, UUID t, long parentId, String rank, long exceptId) {
        return j.query("SELECT rank FROM nodes WHERE tenant_id = ? AND parent_id = ? AND rank < ? AND node_id <> ? "
                + "ORDER BY rank DESC LIMIT 1", (rs, i) -> rs.getString(1), t, parentId, rank, exceptId)
                .stream().findFirst();
    }

    // ---------------------------------------------------------------- nodes: writes

    public void insertNode(JdbcTemplate j, UUID t, Node n) {
        j.update("INSERT INTO nodes (tenant_id, node_id, space_id, parent_id, path, depth, rank, node_type, title, "
                        + "status, version, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                t, n.id(), n.spaceId(), n.parentId(), n.path(), n.depth(), n.rank(), n.type(), n.title(),
                n.status().name(), n.version(), n.createdBy());
    }

    public void upsertBody(JdbcTemplate j, UUID t, long id, String body) {
        j.update("INSERT INTO node_content (tenant_id, node_id, body) VALUES (?, ?, ?) "
                + "ON CONFLICT (tenant_id, node_id) DO UPDATE SET body = excluded.body, "
                + "body_version = node_content.body_version + 1", t, id, body);
    }

    /** OCC: 0 rows means someone else changed the node since {@code expectedVersion} was read. */
    public int updateTitle(JdbcTemplate j, UUID t, long id, long expectedVersion, @Nullable String title) {
        return j.update("UPDATE nodes SET title = coalesce(?, title), version = version + 1, updated_at = now() "
                + "WHERE tenant_id = ? AND node_id = ? AND version = ?", title, t, id, expectedVersion);
    }

    /** Trash is O(1): one row. Descendants are hidden because reads check every id on the path. */
    public int setStatus(JdbcTemplate j, UUID t, long id, long expectedVersion, NodeStatus status) {
        return j.update("UPDATE nodes SET status = ?, trashed_at = CASE WHEN ? = 'TRASHED' THEN now() END, "
                        + "version = version + 1, updated_at = now() WHERE tenant_id = ? AND node_id = ? AND version = ?",
                status.name(), status.name(), t, id, expectedVersion);
    }

    public int setRank(JdbcTemplate j, UUID t, long id, long expectedVersion, String rank) {
        return j.update("UPDATE nodes SET rank = ?, version = version + 1, updated_at = now() "
                + "WHERE tenant_id = ? AND node_id = ? AND version = ?", rank, t, id, expectedVersion);
    }

    // ---------------------------------------------------------------- move

    /** Up to {@code limit} rows under the prefix: the "is this a small move?" probe without counting a huge subtree. */
    public int countUnder(JdbcTemplate j, UUID t, String prefix, int limit) {
        return Objects.requireNonNull(j.queryForObject("SELECT count(*) FROM (SELECT 1 FROM nodes WHERE tenant_id = ? "
                + "AND path >= ? AND path < ? LIMIT ?) x", Integer.class, t, prefix, Paths.upperBound(prefix), limit));
    }

    public int maxDepthUnder(JdbcTemplate j, UUID t, String prefix) {
        return Objects.requireNonNull(j.queryForObject("SELECT coalesce(max(depth), 0) FROM nodes "
                        + "WHERE tenant_id = ? AND path >= ? AND path < ?",
                Integer.class, t, prefix, Paths.upperBound(prefix)));
    }

    /** The small move: one set-based statement rewrites the prefix of every row in the subtree, root included. */
    public void rewritePrefix(JdbcTemplate j, UUID t, String oldPrefix, String newPrefix, int depthDelta,
                              long newSpaceId) {
        j.update("UPDATE nodes SET path = ? || substr(path, ?), depth = depth + ?, space_id = ?, "
                        + "updated_at = now() WHERE tenant_id = ? AND path >= ? AND path < ?",
                newPrefix, oldPrefix.length() + 1, depthDelta, newSpaceId, t, oldPrefix, Paths.upperBound(oldPrefix));
    }

    /** Only the moved root changes parent and rank; its descendants keep their parents. */
    public void reparent(JdbcTemplate j, UUID t, long id, long newParentId, String rank) {
        j.update("UPDATE nodes SET parent_id = ?, rank = ?, version = version + 1, updated_at = now() "
                + "WHERE tenant_id = ? AND node_id = ?", newParentId, rank, t, id);
    }

    /** Large move, accept step: the root alone moves now; the overlay covers its descendants. */
    public void moveRootOnly(JdbcTemplate j, UUID t, long id, long newParentId, String newPath, int newDepth,
                             String rank) {
        j.update("UPDATE nodes SET parent_id = ?, path = ?, depth = ?, rank = ?, version = version + 1, "
                + "updated_at = now() WHERE tenant_id = ? AND node_id = ?", newParentId, newPath, newDepth, rank, t, id);
    }

    /**
     * One background batch. Rewritten rows leave the old prefix's range, so the loop needs no cursor. Rows a user is
     * editing right now are skipped and picked up by a later batch.
     */
    public int rewriteBatch(JdbcTemplate j, UUID t, MoveJob job, int batchSize) {
        return j.update("WITH batch AS (SELECT node_id FROM nodes WHERE tenant_id = ? AND path >= ? AND path < ? "
                        + "ORDER BY path LIMIT ? FOR UPDATE SKIP LOCKED) "
                        + "UPDATE nodes n SET path = ? || substr(n.path, ?), depth = n.depth + ? "
                        + "FROM batch WHERE n.tenant_id = ? AND n.node_id = batch.node_id",
                t, job.oldPrefix(), Paths.upperBound(job.oldPrefix()), batchSize,
                job.newPrefix(), job.oldPrefix().length() + 1, job.depthDelta(), t);
    }

    public Overlay overlay(JdbcTemplate j, UUID t, long spaceId) {
        return runningJob(j, t, spaceId).map(MoveJob::overlay).orElse(Overlay.NONE);
    }

    public Optional<MoveJob> runningJob(JdbcTemplate j, UUID t, long spaceId) {
        return j.query("SELECT " + JOB_COLS + " FROM move_jobs WHERE tenant_id = ? AND space_id = ? "
                + "AND state = 'RUNNING'", JOB, t, spaceId).stream().findFirst();
    }

    public void insertMoveJob(JdbcTemplate j, UUID t, MoveJob job) {
        j.update("INSERT INTO move_jobs (tenant_id, job_id, space_id, root_node_id, old_prefix, new_prefix, "
                        + "depth_delta, state) VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING')",
                t, job.id(), job.spaceId(), job.rootNodeId(), job.oldPrefix(), job.newPrefix(), job.depthDelta());
    }

    public Optional<MoveJob> moveJob(JdbcTemplate j, UUID t, long jobId) {
        return j.query("SELECT " + JOB_COLS + " FROM move_jobs WHERE tenant_id = ? AND job_id = ?", JOB, t, jobId)
                .stream().findFirst();
    }

    /** A RUNNING job and the tenant it belongs to, as claimed by one worker. */
    public record ClaimedJob(UUID tenantId, MoveJob job) {
    }

    /** Takes the oldest RUNNING job whose lease is free or expired (its worker died). */
    public Optional<ClaimedJob> claimJob(JdbcTemplate j, String owner, Duration lease) {
        return j.query("UPDATE move_jobs m SET lease_owner = ?, lease_until = now() + ? * interval '1 millisecond' "
                        + "FROM (SELECT tenant_id, job_id FROM move_jobs WHERE state = 'RUNNING' "
                        + "  AND (lease_until IS NULL OR lease_until < now()) ORDER BY created_at LIMIT 1 "
                        + "  FOR UPDATE SKIP LOCKED) c "
                        + "WHERE m.tenant_id = c.tenant_id AND m.job_id = c.job_id RETURNING m.tenant_id, "
                        + JOB_COLS.replaceAll("(\\w+)", "m.$1"),
                (rs, i) -> new ClaimedJob(rs.getObject("tenant_id", UUID.class), JOB.mapRow(rs, i)),
                owner, lease.toMillis()).stream().findFirst();
    }

    /** Records progress and extends the lease; false if another worker has taken the job over. */
    public boolean renewLease(JdbcTemplate j, UUID t, long jobId, String owner, Duration lease, long rowsDone) {
        return j.update("UPDATE move_jobs SET rows_done = rows_done + ?, "
                        + "lease_until = now() + ? * interval '1 millisecond' "
                        + "WHERE tenant_id = ? AND job_id = ? AND lease_owner = ? AND state = 'RUNNING'",
                rowsDone, lease.toMillis(), t, jobId, owner) == 1;
    }

    public void markDone(JdbcTemplate j, UUID t, long jobId) {
        j.update("UPDATE move_jobs SET state = 'DONE', finished_at = now(), lease_owner = NULL, lease_until = NULL "
                + "WHERE tenant_id = ? AND job_id = ?", t, jobId);
    }

    // ---------------------------------------------------------------- purge

    /**
     * Deletes up to {@code limit} rows of the subtree, deepest first, with their content and restrictions. Deepest
     * first means a batch never deletes a parent while one of its children survives, so the parent FK holds.
     */
    public int deleteBatch(JdbcTemplate j, UUID t, String prefix, int limit) {
        return j.update("WITH doomed AS (SELECT node_id FROM nodes WHERE tenant_id = ? AND path >= ? AND path < ? "
                        + "  ORDER BY depth DESC LIMIT ?), "
                        + "c AS (DELETE FROM node_content x USING doomed d WHERE x.tenant_id = ? AND x.node_id = d.node_id), "
                        + "r AS (DELETE FROM restrictions x USING doomed d WHERE x.tenant_id = ? AND x.node_id = d.node_id) "
                        + "DELETE FROM nodes n USING doomed d WHERE n.tenant_id = ? AND n.node_id = d.node_id",
                t, prefix, Paths.upperBound(prefix), limit, t, t, t);
    }

    // ---------------------------------------------------------------- restrictions

    /** Restriction rows on any of {@code ids}: node to op to allowed principals. Nodes without rows are absent. */
    public Map<Long, Map<RestrictionOp, Set<String>>> restrictions(JdbcTemplate j, UUID t, Collection<Long> ids) {
        Map<Long, Map<RestrictionOp, Set<String>>> out = new HashMap<>();
        if (ids.isEmpty()) {
            return out;
        }
        j.query("SELECT node_id, op, principal FROM restrictions WHERE tenant_id = ? AND node_id = ANY (?::bigint[])",
                rs -> {
                    out.computeIfAbsent(rs.getLong(1), k -> new EnumMap<>(RestrictionOp.class))
                            .computeIfAbsent(RestrictionOp.parse(rs.getString(2)), k -> new LinkedHashSet<>())
                            .add(rs.getString(3));
                }, t, idArray(ids));
        return out;
    }

    public void replaceRestrictions(JdbcTemplate j, UUID t, long id, RestrictionOp op, Collection<String> principals) {
        j.update("DELETE FROM restrictions WHERE tenant_id = ? AND node_id = ? AND op = ?", t, id, op.db());
        for (String p : new LinkedHashSet<>(principals)) {
            j.update("INSERT INTO restrictions (tenant_id, node_id, op, principal) VALUES (?, ?, ?, ?)",
                    t, id, op.db(), p);
        }
    }

    // ---------------------------------------------------------------- idempotency

    /** The node creation a key was first used for. */
    public record IdempotencyRecord(String requestHash, long nodeId) {
    }

    /**
     * Claims the key in the writer's transaction. If another transaction holds the same key uncommitted, this waits for
     * it: on its commit the claim fails (replay its node), on its rollback the claim succeeds.
     */
    public boolean claimIdempotencyKey(JdbcTemplate j, UUID t, String key, String requestHash, long nodeId) {
        return j.update("INSERT INTO idempotency_keys (tenant_id, key, request_hash, node_id) VALUES (?, ?, ?, ?) "
                + "ON CONFLICT DO NOTHING", t, key, requestHash, nodeId) == 1;
    }

    public Optional<IdempotencyRecord> idempotencyKey(JdbcTemplate j, UUID t, String key) {
        return j.query("SELECT request_hash, node_id FROM idempotency_keys WHERE tenant_id = ? AND key = ?",
                (rs, i) -> new IdempotencyRecord(rs.getString(1), rs.getLong(2)), t, key).stream().findFirst();
    }

    public int pruneIdempotencyKeys(JdbcTemplate j, Duration olderThan) {
        return j.update("DELETE FROM idempotency_keys WHERE created_at < now() - ? * interval '1 millisecond'",
                olderThan.toMillis());
    }

    // ---------------------------------------------------------------- outbox

    private record Payload(long spaceId, long nodeId, @Nullable Long parentId, @Nullable Long oldParentId,
                           @Nullable Long oldSpaceId, long oldTreeVersion, @Nullable Long jobId, long treeVersion) {
    }

    /** The last write of every mutating transaction, so the event commits or rolls back with the change. */
    public void appendEvent(JdbcTemplate j, TreeEvent e) {
        Payload p = new Payload(e.spaceId(), e.nodeId(), e.parentId(), e.oldParentId(), e.oldSpaceId(),
                e.oldTreeVersion(), e.jobId(), e.treeVersion());
        try {
            j.update("INSERT INTO outbox (tenant_id, event_type, payload) VALUES (?, ?, ?::jsonb)",
                    e.tenantId(), e.type().name(), json.writeValueAsString(p));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** Takes the shard's relay lease for this transaction; false if another pod is relaying right now. */
    public boolean lockRelay(JdbcTemplate j) {
        return !j.query("SELECT id FROM outbox_relay_state WHERE id = 1 FOR UPDATE SKIP LOCKED", (rs, i) -> 1)
                .isEmpty();
    }

    public List<TreeEvent> outboxBatch(JdbcTemplate j, int limit) {
        return j.query("SELECT seq, tenant_id, event_type, payload::text, created_at FROM outbox ORDER BY seq LIMIT ?",
                (rs, i) -> {
                    Payload p = readPayload(rs.getString(4));
                    return new TreeEvent(rs.getLong(1), rs.getObject(2, UUID.class),
                            TreeEvent.Type.valueOf(rs.getString(3)), p.spaceId(), p.nodeId(), p.parentId(),
                            p.oldParentId(), p.oldSpaceId(), p.oldTreeVersion(), p.jobId(), p.treeVersion(),
                            rs.getTimestamp(5).toInstant());
                }, limit);
    }

    /** Deletes exactly the relayed rows; a row that committed late with a smaller seq stays for the next batch. */
    public void deleteRelayed(JdbcTemplate j, Collection<Long> seqs) {
        j.update("DELETE FROM outbox WHERE seq = ANY (?::bigint[])", idArray(seqs));
        j.update("UPDATE outbox_relay_state SET relayed_seq = greatest(relayed_seq, ?) WHERE id = 1",
                seqs.stream().mapToLong(Long::longValue).max().orElse(0));
    }

    /** Age of the oldest unrelayed event, in ms; 0 when the outbox is empty. */
    public long outboxLagMs(JdbcTemplate j) {
        Long lag = j.queryForObject("SELECT coalesce((extract(epoch FROM now() - min(created_at)) * 1000)::bigint, 0) "
                + "FROM outbox", Long.class);
        return lag == null ? 0 : lag;
    }

    private Payload readPayload(String text) {
        try {
            return json.readValue(text, Payload.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt outbox payload: " + text, e);
        }
    }

    // ---------------------------------------------------------------- helpers

    /** {@code {1,2,3}}: ids are numbers, so a text array literal is safe and keeps JdbcTemplate's simple binding. */
    static String idArray(Collection<Long> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
    }

    private static Node node(ResultSet rs, String pathCol, String depthCol) throws SQLException {
        long parent = rs.getLong("parent_id");
        Long parentId = rs.wasNull() ? null : parent;
        return new Node(rs.getLong("node_id"), rs.getLong("space_id"), parentId, rs.getString(pathCol),
                rs.getInt(depthCol), rs.getString("rank"), rs.getString("node_type"), rs.getString("title"),
                NodeStatus.valueOf(rs.getString("status")), rs.getInt("version"), rs.getString("created_by"),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant());
    }

    @Nullable
    private static java.time.Instant instant(@Nullable Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
