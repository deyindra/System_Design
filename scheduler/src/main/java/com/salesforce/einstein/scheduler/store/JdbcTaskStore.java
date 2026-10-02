package com.salesforce.einstein.scheduler.store;

import com.salesforce.einstein.scheduler.Recurrence;
import com.salesforce.einstein.scheduler.RecurrenceUnit;
import com.salesforce.einstein.scheduler.spi.FinishResult;
import com.salesforce.einstein.scheduler.spi.Outcome;
import com.salesforce.einstein.scheduler.spi.RemoveResult;
import com.salesforce.einstein.scheduler.spi.RunOutcome;
import com.salesforce.einstein.scheduler.spi.StoreCounts;
import com.salesforce.einstein.scheduler.spi.TaskRecord;
import com.salesforce.einstein.scheduler.spi.TaskSpec;
import com.salesforce.einstein.scheduler.spi.TaskState;
import com.salesforce.einstein.scheduler.spi.TaskStore;
import com.salesforce.einstein.scheduler.spi.TaskStoreException;
import com.salesforce.einstein.scheduler.spi.TaskTransitions;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;

/**
 * Multi-VM store over a SQL database. Uses standard SQL (identity column, {@code FETCH FIRST},
 * {@code SELECT … FOR UPDATE}); tested on H2 and runs as-is on PostgreSQL. Databases without those
 * features (e.g. MySQL: {@code LIMIT}, {@code AUTO_INCREMENT}) need a subclass-free copy with adjusted SQL.
 * Every scheduler node gets its own instance pointing at the same database.
 *
 * <p>Schema (version {@value #SCHEMA_VERSION}), split by how often each part changes:
 * <ul>
 *   <li>{@code task}: the definition, written once at submit (type, routing, priority, timeout, recurrence).</li>
 *   <li>{@code task_payload}: the job payload, kept out of the hot rows; read only when a task is claimed.</li>
 *   <li>{@code task_schedule}: the queue row (state, next run, timeout streak, version), the hot table.</li>
 *   <li>{@code task_lease}: one row per running claim (owner, expiry), so the reaper scans only running tasks.</li>
 *   <li>{@code outcome}: the outbox, one final result per task, addressed to the submitting node.</li>
 *   <li>{@code meta}: the live-task counter and the schema version.</li>
 * </ul>
 * The child tables reference {@code task} with {@code ON DELETE CASCADE}, so deleting a finished task
 * removes all of its rows at once.
 *
 * <ul>
 *   <li><b>Claim:</b> pick due candidates in schedule order, then take each with a conditional
 *       {@code UPDATE task_schedule … WHERE version = ? AND state = 'SCHEDULED'} plus an insert into
 *       {@code task_lease}; exactly one node wins a row.</li>
 *   <li><b>Lease:</b> a claim is valid until {@code now + timeout + leaseGrace}; any node's reaper settles
 *       expired leases (crashed / paused / partitioned owners).</li>
 *   <li><b>Fencing:</b> {@code finish}/{@code remove} lock the schedule row ({@code SELECT … FOR UPDATE})
 *       and check the version, so a stale owner can't overwrite a result.</li>
 *   <li><b>Capacity:</b> a counter row updated with {@code val = val + 1 WHERE val < maxTasks}, so the
 *       cluster-wide {@code maxTasks} bound holds without races.</li>
 * </ul>
 *
 * <p>Pass a pooled {@link DataSource} (e.g. HikariCP) in production. Call {@link #createSchema()} once
 * (idempotent) or apply the same DDL with your migration tool.
 */
// Table names are built at runtime from tablePrefix, so the IDE can't resolve them against a data source.
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public final class JdbcTaskStore implements TaskStore {
    /** Version of the table layout below, stored in {@code meta}; bump it with every DDL change. */
    public static final int SCHEMA_VERSION = 2;
    /** Longest payload accepted by {@link #insert}; the column is sized to match. */
    public static final int MAX_PAYLOAD_CHARS = 16_000;
    /** Outcome details longer than this are cut. */
    private static final int MAX_DETAIL_CHARS = 4000;

    private final DataSource ds;
    private final String task, payload, schedule, lease, outcome, meta;   // table names
    private final String selectRecord;
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    public JdbcTaskStore(DataSource ds) { this(ds, "sched_"); }

    /** @param tablePrefix lets several independent schedulers share one database */
    public JdbcTaskStore(DataSource ds, String tablePrefix) {
        this.ds = java.util.Objects.requireNonNull(ds, "dataSource");
        if (!tablePrefix.matches("[A-Za-z0-9_]*"))
            throw new IllegalArgumentException("tablePrefix must match [A-Za-z0-9_]*");
        this.task = tablePrefix + "task";
        this.payload = tablePrefix + "task_payload";
        this.schedule = tablePrefix + "task_schedule";
        this.lease = tablePrefix + "task_lease";
        this.outcome = tablePrefix + "outcome";
        this.meta = tablePrefix + "meta";
        this.selectRecord = "SELECT t.task_id, t.job_type, p.payload, t.pinned_node, t.submitter_node, t.priority,"
                + " t.timeout_ms, t.rec_unit, t.rec_amount, t.zone_id, t.max_timeouts, s.next_run_ms, s.state,"
                + " s.consecutive_timeouts, s.removal_requested, l.owner_node, l.lease_until_ms, s.version"
                + " FROM " + task + " t JOIN " + schedule + " s ON s.task_id = t.task_id"
                + " LEFT JOIN " + payload + " p ON p.task_id = t.task_id"
                + " LEFT JOIN " + lease + " l ON l.task_id = t.task_id"
                + " WHERE t.task_id = ?";
    }

    /**
     * Creates the tables if missing and checks the schema version. Safe to call from every node at
     * startup. Throws {@link IllegalStateException} if the database holds a different schema version,
     * so a node never runs against a layout it doesn't understand.
     */
    public JdbcTaskStore createSchema() {
        String ref = " BIGINT PRIMARY KEY REFERENCES " + task + " (task_id) ON DELETE CASCADE";
        tx(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS " + task + " ("
                        + " task_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,"
                        + " job_type VARCHAR(200) NOT NULL, pinned_node VARCHAR(200),"
                        + " submitter_node VARCHAR(200) NOT NULL, priority INT NOT NULL, timeout_ms BIGINT NOT NULL,"
                        + " rec_unit VARCHAR(16), rec_amount INT, zone_id VARCHAR(64) NOT NULL,"
                        + " max_timeouts INT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)");
                s.execute("CREATE TABLE IF NOT EXISTS " + payload + " (task_id" + ref + ","
                        + " payload VARCHAR(" + MAX_PAYLOAD_CHARS + ") NOT NULL)");
                s.execute("CREATE TABLE IF NOT EXISTS " + schedule + " (task_id" + ref + ","
                        + " state VARCHAR(16) NOT NULL, next_run_ms BIGINT NOT NULL,"
                        + " consecutive_timeouts INT NOT NULL, removal_requested BOOLEAN NOT NULL,"
                        + " version BIGINT NOT NULL)");
                s.execute("CREATE INDEX IF NOT EXISTS " + schedule + "_due ON " + schedule + " (state, next_run_ms)");
                s.execute("CREATE TABLE IF NOT EXISTS " + lease + " (task_id" + ref + ","
                        + " owner_node VARCHAR(200) NOT NULL, lease_until_ms BIGINT NOT NULL,"
                        + " claimed_ms BIGINT NOT NULL)");
                s.execute("CREATE INDEX IF NOT EXISTS " + lease + "_expiry ON " + lease + " (lease_until_ms)");
                s.execute("CREATE INDEX IF NOT EXISTS " + lease + "_owner ON " + lease + " (owner_node)");
                s.execute("CREATE TABLE IF NOT EXISTS " + outcome + " (task_id BIGINT PRIMARY KEY,"
                        + " submitter_node VARCHAR(200) NOT NULL, kind VARCHAR(16) NOT NULL,"
                        + " detail VARCHAR(" + MAX_DETAIL_CHARS + "),"
                        + " created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)");
                s.execute("CREATE INDEX IF NOT EXISTS " + outcome + "_sub ON " + outcome + " (submitter_node)");
                s.execute("CREATE TABLE IF NOT EXISTS " + meta + " (name VARCHAR(32) PRIMARY KEY, val BIGINT NOT NULL)");
            }
            return null;
        });
        try {
            tx(c -> {
                Long version = null, live = null;
                try (PreparedStatement ps = c.prepareStatement("SELECT name, val FROM " + meta
                        + " WHERE name IN ('live', 'schema_version')");
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        if (rs.getString(1).equals("live")) live = rs.getLong(2);
                        else version = rs.getLong(2);
                    }
                }
                if (version == null && live != null)
                    throw new IllegalStateException(meta + " predates schema versioning;"
                            + " migrate the tables to schema version " + SCHEMA_VERSION);
                if (version != null && version != SCHEMA_VERSION)
                    throw new IllegalStateException("database has schema version " + version
                            + ", this code needs " + SCHEMA_VERSION);
                if (version == null) {                       // fresh database: both rows in one transaction
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + meta + " (name, val) VALUES (?, ?)")) {
                        ps.setString(1, "live");
                        ps.setLong(2, 0);
                        ps.addBatch();
                        ps.setString(1, "schema_version");
                        ps.setLong(2, SCHEMA_VERSION);
                        ps.addBatch();
                        ps.executeBatch();
                    }
                }
                return null;
            });
        } catch (TaskStoreException raced) {
            // another node inserted the meta rows concurrently: fine
        }
        return this;
    }

    @Override public boolean isShared() { return true; }

    @Override public TaskRecord insert(TaskSpec s, int maxTasks) {
        if (s.payload() != null && s.payload().length() > MAX_PAYLOAD_CHARS)
            throw new IllegalArgumentException("payload longer than " + MAX_PAYLOAD_CHARS + " characters");
        TaskRecord rec = tx(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE " + meta + " SET val = val + 1 WHERE name = 'live' AND val < ?")) {
                ps.setLong(1, maxTasks);
                if (ps.executeUpdate() == 0) throw new RejectedExecutionException("capacity " + maxTasks + " reached");
            }
            long id;
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + task + " (job_type, pinned_node,"
                    + " submitter_node, priority, timeout_ms, rec_unit, rec_amount, zone_id, max_timeouts)"
                    + " VALUES (?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, s.jobType());
                ps.setString(2, s.pinnedNode());
                ps.setString(3, s.submitterNode());
                ps.setInt(4, s.priority());
                ps.setLong(5, s.timeoutMillis());
                if (s.recurrence() == null) {
                    ps.setNull(6, Types.VARCHAR);
                    ps.setNull(7, Types.INTEGER);
                } else {
                    ps.setString(6, s.recurrence().unit().name());
                    ps.setInt(7, s.recurrence().amount());
                }
                ps.setString(8, s.zone().getId());
                ps.setInt(9, s.maxConsecutiveTimeouts());
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    keys.next();
                    id = keys.getLong(1);
                }
            }
            if (s.payload() != null) {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + payload + " (task_id, payload) VALUES (?,?)")) {
                    ps.setLong(1, id);
                    ps.setString(2, s.payload());
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + schedule + " (task_id, state, next_run_ms,"
                    + " consecutive_timeouts, removal_requested, version) VALUES (?, 'SCHEDULED', ?, 0, FALSE, 0)")) {
                ps.setLong(1, id);
                ps.setLong(2, s.firstRunMillis());
                ps.executeUpdate();
            }
            return TaskRecord.newTask(id, s);
        });
        fireChanged();
        return rec;
    }

    @Override public List<TaskRecord> claimDue(String nodeId, Set<String> jobTypes, long nowMillis,
                                               int limit, long leaseGraceMillis) {
        if (limit <= 0) return List.of();
        List<long[]> candidates = tx(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT s.task_id, s.version FROM " + schedule + " s"
                    + " JOIN " + task + " t ON t.task_id = s.task_id"
                    + " WHERE s.state = 'SCHEDULED' AND s.next_run_ms <= ? AND " + claimable(jobTypes)
                    + " ORDER BY s.next_run_ms, t.priority DESC, s.task_id FETCH FIRST " + limit + " ROWS ONLY")) {
                ps.setLong(1, nowMillis);
                bindClaimable(ps, 2, nodeId, jobTypes);
                List<long[]> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) out.add(new long[]{rs.getLong(1), rs.getLong(2)});
                }
                return out;
            }
        });
        List<TaskRecord> claimed = new ArrayList<>();
        for (long[] candidate : candidates) {
            TaskRecord r = tx(c -> {
                try (PreparedStatement ps = c.prepareStatement("UPDATE " + schedule + " SET state = 'RUNNING',"
                        + " version = version + 1 WHERE task_id = ? AND version = ? AND state = 'SCHEDULED'")) {
                    ps.setLong(1, candidate[0]);
                    ps.setLong(2, candidate[1]);
                    if (ps.executeUpdate() == 0) return null;   // another node won the row
                }
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + lease + " (task_id, owner_node,"
                        + " lease_until_ms, claimed_ms) SELECT task_id, ?, ? + timeout_ms, ? FROM " + task
                        + " WHERE task_id = ?")) {
                    ps.setString(1, nodeId);
                    ps.setLong(2, nowMillis + leaseGraceMillis);
                    ps.setLong(3, nowMillis);
                    ps.setLong(4, candidate[0]);
                    ps.executeUpdate();
                }
                return select(c, candidate[0]);
            });
            if (r != null) claimed.add(r);
        }
        return claimed;
    }

    @Override public long nextDueMillis(String nodeId, Set<String> jobTypes) {
        return tx(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT MIN(s.next_run_ms) FROM " + schedule + " s"
                    + " JOIN " + task + " t ON t.task_id = s.task_id"
                    + " WHERE s.state = 'SCHEDULED' AND " + claimable(jobTypes))) {
                bindClaimable(ps, 1, nodeId, jobTypes);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    long v = rs.getLong(1);
                    return rs.wasNull() ? Long.MAX_VALUE : v;
                }
            }
        });
    }

    @Override public FinishResult finish(long taskId, long version, RunOutcome result, String detail,
                                         long nowMillis, boolean nodeStopping) {
        FinishResult fr = tx(c -> {
            TaskRecord before = lockAndSelect(c, taskId);
            if (before == null || before.state() != TaskState.RUNNING || before.version() != version)
                return FinishResult.stale();
            TaskTransitions.Transition t = TaskTransitions.finish(before, result, detail, nowMillis, nodeStopping, true);
            write(c, t.after(), t.outcome());
            return new FinishResult(true, t.after(), t.outcome());
        });
        if (fr.applied()) fireChanged();
        return fr;
    }

    @Override public List<FinishResult> reapExpiredLeases(long nowMillis) {
        List<String[]> expired = tx(c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT l.task_id, s.version, l.owner_node FROM " + lease
                    + " l JOIN " + schedule + " s ON s.task_id = l.task_id WHERE l.lease_until_ms < ?")) {
                ps.setLong(1, nowMillis);
                List<String[]> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) out.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3)});
                }
                return out;
            }
        });
        List<FinishResult> settled = new ArrayList<>();
        for (String[] e : expired) {
            FinishResult fr = finish(Long.parseLong(e[0]), Long.parseLong(e[1]), RunOutcome.LEASE_EXPIRED,
                    "lease expired (owner " + e[2] + ")", nowMillis, false);
            if (fr.applied()) settled.add(fr);                 // otherwise another node reaped it first
        }
        return settled;
    }

    @Override public RemoveResult remove(long taskId) {
        RemoveResult rr = tx(c -> {
            TaskRecord before = lockAndSelect(c, taskId);
            if (before == null) return new RemoveResult(RemoveResult.Kind.NOT_FOUND, null, null);
            RemoveResult r = TaskTransitions.remove(before);
            if (r.removed()) write(c, r.after(), r.outcome());
            return r;
        });
        if (rr.removed()) fireChanged();
        return rr;
    }

    @Override public boolean reschedule(long taskId, long newRunMillis) {
        boolean ok = tx(c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE " + schedule
                    + " SET next_run_ms = ?, version = version + 1 WHERE task_id = ? AND state = 'SCHEDULED'")) {
                ps.setLong(1, newRunMillis);
                ps.setLong(2, taskId);
                return ps.executeUpdate() == 1;
            }
        });
        if (ok) fireChanged();
        return ok;
    }

    @Override public List<Outcome> takeOutcomes(String submitterNode) {
        return tx(c -> {
            List<Outcome> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT task_id, kind, detail FROM " + outcome
                    + " WHERE submitter_node = ?")) {
                ps.setString(1, submitterNode);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next())
                        out.add(new Outcome(rs.getLong(1), submitterNode, Outcome.Kind.valueOf(rs.getString(2)),
                                rs.getString(3)));
                }
            }
            // Delete exactly what was read: a row committed after the SELECT stays for the next call.
            // The IDE inspects only the literal before the table name, so it misses the WHERE clause.
            //noinspection SqlWithoutWhere
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + outcome + " WHERE task_id = ?")) {
                for (Outcome o : out) {
                    ps.setLong(1, o.taskId());
                    ps.addBatch();
                }
                if (!out.isEmpty()) ps.executeBatch();
            }
            return out;
        });
    }

    @Override public Optional<TaskRecord> find(long taskId) {
        return Optional.ofNullable(tx(c -> select(c, taskId)));
    }

    @Override public StoreCounts counts() {
        return tx(c -> {
            int pending = 0, running = 0, blacklisted = 0;
            try (PreparedStatement ps = c.prepareStatement("SELECT state, COUNT(*) FROM " + schedule + " GROUP BY state");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int n = rs.getInt(2);
                    switch (TaskState.valueOf(rs.getString(1))) {
                        case SCHEDULED -> pending = n;
                        case RUNNING -> running = n;
                        case BLACKLISTED -> blacklisted = n;
                        default -> { }
                    }
                }
            }
            return new StoreCounts(pending, running, blacklisted, pending + running + blacklisted);
        });
    }

    @Override public void addChangeListener(Runnable listener) { listeners.add(listener); }

    // ---- SQL helpers --------------------------------------------------------

    /** Pinned to this node, or unpinned with a job type this node has a handler for. */
    private static String claimable(Set<String> jobTypes) {
        if (jobTypes.isEmpty()) return "t.pinned_node = ?";
        return "(t.pinned_node = ? OR (t.pinned_node IS NULL AND t.job_type IN ("
                + String.join(",", java.util.Collections.nCopies(jobTypes.size(), "?")) + ")))";
    }

    private static void bindClaimable(PreparedStatement ps, int from, String nodeId, Set<String> jobTypes)
            throws SQLException {
        ps.setString(from++, nodeId);
        for (String t : jobTypes) ps.setString(from++, t);
    }

    private TaskRecord select(Connection c, long taskId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(selectRecord)) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? read(rs) : null;
            }
        }
    }

    /**
     * Locks the task's schedule row for this transaction, then reads the whole record. Every change to
     * a task (claim, finish, remove, reschedule) goes through that row, so it serializes them.
     */
    private TaskRecord lockAndSelect(Connection c, long taskId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT version FROM " + schedule
                + " WHERE task_id = ? FOR UPDATE")) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
            }
        }
        return select(c, taskId);
    }

    /**
     * Persists a transition. A terminal task is deleted (its schedule, payload and lease rows go with it
     * through the cascade) and frees a capacity slot; a task leaving RUNNING drops its lease row.
     * Outcomes go to the outbox.
     */
    private void write(Connection c, TaskRecord r, Outcome o) throws SQLException {
        if (r.state().isTerminal()) {
            //noinspection SqlWithoutWhere
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + task + " WHERE task_id = ?")) {
                ps.setLong(1, r.taskId());
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE " + meta + " SET val = val - 1 WHERE name = 'live'")) {
                ps.executeUpdate();
            }
        } else {
            try (PreparedStatement ps = c.prepareStatement("UPDATE " + schedule + " SET next_run_ms = ?, state = ?,"
                    + " consecutive_timeouts = ?, removal_requested = ?, version = ? WHERE task_id = ?")) {
                ps.setLong(1, r.nextRunMillis());
                ps.setString(2, r.state().name());
                ps.setInt(3, r.consecutiveTimeouts());
                ps.setBoolean(4, r.removalRequested());
                ps.setLong(5, r.version());
                ps.setLong(6, r.taskId());
                ps.executeUpdate();
            }
            if (r.state() != TaskState.RUNNING) {
                //noinspection SqlWithoutWhere
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + lease + " WHERE task_id = ?")) {
                    ps.setLong(1, r.taskId());
                    ps.executeUpdate();
                }
            }
        }
        if (o != null) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + outcome
                    + " (task_id, submitter_node, kind, detail) VALUES (?,?,?,?)")) {
                ps.setLong(1, o.taskId());
                ps.setString(2, o.submitterNode());
                ps.setString(3, o.kind().name());
                ps.setString(4, truncate(o.detail()));
                ps.executeUpdate();
            }
        }
    }

    private static TaskRecord read(ResultSet rs) throws SQLException {
        String unit = rs.getString("rec_unit");
        Recurrence rec = unit == null ? null : new Recurrence(RecurrenceUnit.valueOf(unit), rs.getInt("rec_amount"));
        return new TaskRecord(rs.getLong("task_id"), rs.getString("job_type"), rs.getString("payload"),
                rs.getString("pinned_node"), rs.getString("submitter_node"), rs.getInt("priority"),
                rs.getLong("timeout_ms"), rec, ZoneId.of(rs.getString("zone_id")), rs.getInt("max_timeouts"),
                rs.getLong("next_run_ms"), TaskState.valueOf(rs.getString("state")),
                rs.getInt("consecutive_timeouts"), rs.getBoolean("removal_requested"),
                rs.getString("owner_node"), rs.getLong("lease_until_ms"), rs.getLong("version"));
    }

    private static String truncate(String s) {
        return s == null || s.length() <= MAX_DETAIL_CHARS ? s : s.substring(0, MAX_DETAIL_CHARS);
    }

    private void fireChanged() {
        for (Runnable l : listeners) l.run();
    }

    @FunctionalInterface
    private interface TxBody<T> { T run(Connection c) throws SQLException; }

    /** Runs {@code body} in one transaction; RuntimeExceptions (e.g. capacity) roll back and propagate. */
    private <T> T tx(TxBody<T> body) {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                T r = body.run(c);
                c.commit();
                return r;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new TaskStoreException("task store operation failed", e);
        }
    }
}
