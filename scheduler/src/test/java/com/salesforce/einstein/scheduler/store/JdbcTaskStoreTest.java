package com.salesforce.einstein.scheduler.store;

import com.salesforce.einstein.scheduler.H2;
import com.salesforce.einstein.scheduler.Recurrence;
import com.salesforce.einstein.scheduler.RecurrenceUnit;
import com.salesforce.einstein.scheduler.spi.RunOutcome;
import com.salesforce.einstein.scheduler.spi.TaskRecord;
import com.salesforce.einstein.scheduler.spi.TaskSpec;
import com.salesforce.einstein.scheduler.spi.TaskStore;
import com.salesforce.einstein.scheduler.spi.SharedTaskStoreContractTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The SQL runs against a fresh in-memory H2 database per test, so there is no IDE data source to check it against.
@SuppressWarnings("SqlNoDataSourceInspection")
class JdbcTaskStoreTest extends SharedTaskStoreContractTest {
    private DataSource ds;

    @Override protected TaskStore newStore() {
        ds = H2.freshDatabase();
        return H2.store(ds);
    }

    /** A second node: its own store instance over the same database. */
    @Override protected TaskStore peerOf(TaskStore store) { return H2.store(ds); }

    @Test
    void schemaCreationIsIdempotentAndPrefixIsValidated() {
        H2.store(ds);                                    // second createSchema on the same database
        assertEquals(0, store.counts().live());
        assertThrows(IllegalArgumentException.class, () -> new JdbcTaskStore(ds, "x; DROP TABLE y"));
    }

    @Test
    @DisplayName("createSchema refuses a database with another schema version")
    void schemaVersionIsChecked() throws SQLException {
        exec("UPDATE sched_meta SET val = 99 WHERE name = 'schema_version'");
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> H2.store(ds));
        assertTrue(e.getMessage().contains("99"), e.getMessage());

        exec("DELETE FROM sched_meta WHERE name = 'schema_version'");   // looks like a pre-versioning database
        assertThrows(IllegalStateException.class, () -> H2.store(ds));
    }

    @Test
    @DisplayName("rows per table follow the task: lease only while running, nothing left once terminal")
    void rowsFollowTheLifecycle() throws SQLException {
        long id = store.insert(once(NOW, 0), 10).taskId();
        assertEquals(List.of(1, 1, 1, 0, 0), rows(id), "pending: task, payload, schedule");

        TaskRecord r = store.claimDue(NODE, TYPES, NOW, 1, 1000).get(0);
        assertEquals(List.of(1, 1, 1, 1, 0), rows(id), "running: plus a lease");
        assertEquals(NODE, r.ownerNode());
        assertEquals(NOW + 1000 + 100, r.leaseUntilMillis(), "now + leaseGrace + timeout");

        store.finish(id, r.version(), RunOutcome.SUCCEEDED, null, NOW, false);
        assertEquals(List.of(0, 0, 0, 0, 1), rows(id), "terminal: only the outcome is left");
        store.takeOutcomes(NODE);
        assertEquals(List.of(0, 0, 0, 0, 0), rows(id));
        assertEquals(0, store.counts().live());
    }

    @Test
    @DisplayName("a recurring run gives its lease back; a null payload has no payload row")
    void recurringReleasesLease() throws SQLException {
        long id = store.insert(new TaskSpec("t", null, null, NODE, 0, 100,
                new Recurrence(RecurrenceUnit.SECOND, 1),
                ZoneOffset.UTC, 2, NOW), 10).taskId();
        assertEquals(List.of(1, 0, 1, 0, 0), rows(id));
        TaskRecord r = store.claimDue(NODE, TYPES, NOW, 1, 1000).get(0);
        assertEquals(List.of(1, 0, 1, 1, 0), rows(id));
        store.finish(id, r.version(), RunOutcome.SUCCEEDED, null, NOW, false);
        assertEquals(List.of(1, 0, 1, 0, 0), rows(id), "scheduled again, lease released");
    }

    @Test
    @DisplayName("an oversized payload is rejected before touching the database")
    void payloadLimit() {
        String big = "x".repeat(JdbcTaskStore.MAX_PAYLOAD_CHARS + 1);
        assertThrows(IllegalArgumentException.class, () -> store.insert(
                new TaskSpec("t", big, null, NODE, 0, 100, null, ZoneOffset.UTC, 2, NOW), 10));
        assertEquals(0, store.counts().live(), "no capacity slot taken");
    }

    /** Row counts for {@code id} in: task, task_payload, task_schedule, task_lease, outcome. */
    private List<Integer> rows(long id) throws SQLException {
        List<String> tables = List.of("sched_task", "sched_task_payload", "sched_task_schedule", "sched_task_lease",
                "sched_outcome");
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            List<Integer> out = new ArrayList<>();
            for (String t : tables) {
                try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + t + " WHERE task_id = " + id)) {
                    rs.next();
                    out.add(rs.getInt(1));
                }
            }
            return out;
        }
    }

    private void exec(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }
}
