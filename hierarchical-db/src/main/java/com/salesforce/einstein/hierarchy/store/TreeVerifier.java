package com.salesforce.einstein.hierarchy.store;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Checks the invariant the whole design rests on: {@code path} and {@code depth} are derived from {@code parent_id}.
 * Every child's path is its parent's path plus its own id, one level deeper, in the same space. Run it after a crash,
 * from a test, or periodically per space; {@link #repair} rebuilds the derived columns from {@code parent_id}, the
 * source of truth.
 *
 * <p>Only meaningful while the space has no RUNNING move job: un-migrated rows legitimately disagree until it finishes.
 */
public final class TreeVerifier {
    private TreeVerifier() {
    }

    /** Human-readable violations; empty when the space is consistent. */
    public static List<String> violations(JdbcTemplate j, UUID tenant, long spaceId) {
        List<String> out = j.query("SELECT c.node_id, c.path, c.depth, p.path, p.depth, c.space_id, p.space_id "
                        + "FROM nodes c JOIN nodes p ON p.tenant_id = c.tenant_id AND p.node_id = c.parent_id "
                        + "WHERE c.tenant_id = ? AND (c.space_id = ? OR p.space_id = ?) "
                        + "AND (c.path <> p.path || c.node_id || '/' OR c.depth <> p.depth + 1 OR c.space_id <> p.space_id)",
                (rs, i) -> "node " + rs.getLong(1) + " path=" + rs.getString(2) + " depth=" + rs.getInt(3)
                        + " space=" + rs.getLong(6) + " but parent path=" + rs.getString(4) + " depth=" + rs.getInt(5)
                        + " space=" + rs.getLong(7),
                tenant, spaceId, spaceId);
        out.addAll(j.query("SELECT node_id, path, depth FROM nodes WHERE tenant_id = ? AND space_id = ? "
                        + "AND parent_id IS NULL AND (path <> '/' || node_id || '/' OR depth <> 0)",
                (rs, i) -> "root " + rs.getLong(1) + " path=" + rs.getString(2) + " depth=" + rs.getInt(3),
                tenant, spaceId));
        return out;
    }

    /**
     * Recomputes every path and depth of the space from {@code parent_id} with a recursive walk from the root. Takes the
     * space's exclusive lock, so it must run inside a write transaction. Returns the number of rows fixed.
     */
    public static int repair(JdbcTemplate j, UUID tenant, long spaceId, long rootNodeId, Duration lockTimeout) {
        SpaceLocks.exclusive(j, tenant, List.of(spaceId), lockTimeout);
        return j.update("WITH RECURSIVE t AS ("
                        + "  SELECT node_id, ('/' || node_id || '/') COLLATE \"C\" AS path, 0 AS depth "
                        + "  FROM nodes WHERE tenant_id = ? AND node_id = ? "
                        + "  UNION ALL "
                        + "  SELECT n.node_id, (t.path || n.node_id || '/') COLLATE \"C\", t.depth + 1 "
                        + "  FROM nodes n JOIN t ON n.tenant_id = ? AND n.parent_id = t.node_id"
                        + ") UPDATE nodes n SET path = t.path, depth = t.depth FROM t "
                        + "WHERE n.tenant_id = ? AND n.node_id = t.node_id AND (n.path <> t.path OR n.depth <> t.depth)",
                tenant, rootNodeId, tenant, tenant);
    }
}
