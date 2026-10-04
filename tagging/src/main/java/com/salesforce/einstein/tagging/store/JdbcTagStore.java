package com.salesforce.einstein.tagging.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.tagging.domain.Consistency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.salesforce.einstein.tagging.domain.DuplicateTagNameException;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Page;
import com.salesforce.einstein.tagging.domain.StaleVersionException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagQuery;
import com.salesforce.einstein.tagging.domain.Cursors;
import com.salesforce.einstein.tagging.spi.Capability;
import com.salesforce.einstein.tagging.spi.EntityHit;
import com.salesforce.einstein.tagging.spi.IdempotencyRecord;
import com.salesforce.einstein.tagging.spi.Posting;
import com.salesforce.einstein.tagging.spi.ReadPreference;
import com.salesforce.einstein.tagging.spi.TagReader;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.UnitOfWork;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link TagStore} over JDBC for one shard. It speaks portable PostgreSQL SQL ({@code ON CONFLICT DO
 * NOTHING}, {@code FOR UPDATE}, identity columns), which runs on community PostgreSQL, RDS, Aurora-PG,
 * Cloud SQL, AlloyDB, CloudNativePG, and on H2 in PostgreSQL mode for tests.
 *
 * <p>Every statement filters on {@code tenant_id}, the leading key column. The reader and unit of work
 * bind the tenant when they're created, so a cross-tenant access path doesn't exist.
 *
 * <p>Batch statements rely on per-row update counts, so the PostgreSQL driver must not use
 * {@code reWriteBatchedInserts=true}. That mode reports {@code SUCCESS_NO_INFO}, and the store fails fast if it sees it.
 */
@SuppressWarnings("SqlNoDataSourceInspection")   // IntelliJ: the SQL is checked by the H2 tests, not an IDE data source
public final class JdbcTagStore implements TagStore {
    private static final int SCAN_FETCH_SIZE = 5_000;
    /**
     * Upper bound on a write transaction. The relay treats a seq gap older than its gap timeout as a
     * rollback, so no transaction may hold an uncommitted seq that long: the gap timeout must exceed this.
     */
    public static final int WRITE_TIMEOUT_SECONDS = 10;
    private static final Object REPLICA_BEHIND = new Object();
    private static final Logger log = LoggerFactory.getLogger(JdbcTagStore.class);

    private final String shardId;
    private final NamedParameterJdbcTemplate primary;
    private final NamedParameterJdbcTemplate replica;          // null when there's no replica
    private final JdbcTemplate scanJdbc;
    private final TransactionTemplate writeTx;
    private final TransactionTemplate relayTx;
    private final TransactionTemplate readTx;
    private final TransactionTemplate snapshotTx;
    private final TransactionTemplate replicaTx;
    private final ObjectMapper json;
    private final Clock clock;
    private final int usageBuckets;

    public JdbcTagStore(String shardId, DataSource primaryDs, DataSource replicaDs, ObjectMapper json,
                        Clock clock, int usageBuckets) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.json = Objects.requireNonNull(json, "json");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (usageBuckets < 1) {
            throw new IllegalArgumentException("usageBuckets < 1");
        }
        this.usageBuckets = usageBuckets;
        this.primary = new NamedParameterJdbcTemplate(primaryDs);
        this.scanJdbc = new JdbcTemplate(primaryDs);
        this.scanJdbc.setFetchSize(SCAN_FETCH_SIZE);
        DataSourceTransactionManager primaryTm = new DataSourceTransactionManager(primaryDs);
        this.writeTx = new TransactionTemplate(primaryTm);
        this.writeTx.setTimeout(WRITE_TIMEOUT_SECONDS);
        this.relayTx = new TransactionTemplate(primaryTm);   // no timeout: it spans publishing to the broker
        this.readTx = new TransactionTemplate(primaryTm);
        this.readTx.setReadOnly(true);
        this.snapshotTx = new TransactionTemplate(primaryTm);
        this.snapshotTx.setReadOnly(true);
        this.snapshotTx.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        if (replicaDs != null) {
            this.replica = new NamedParameterJdbcTemplate(replicaDs);
            this.replicaTx = new TransactionTemplate(new DataSourceTransactionManager(replicaDs));
            this.replicaTx.setReadOnly(true);
        } else {
            this.replica = null;
            this.replicaTx = null;
        }
    }

    @Override
    public String shardId() {
        return shardId;
    }

    @Override
    public Set<Capability> capabilities() {
        EnumSet<Capability> caps = EnumSet.of(Capability.ATOMIC_OUTBOX, Capability.SNAPSHOT_READS);
        if (replica != null) {
            caps.add(Capability.READ_REPLICAS);
        }
        return caps;
    }

    @Override
    public <T> T write(String tenantId, Function<UnitOfWork, T> work) {
        return writeTx.execute(status -> work.apply(new Uow(tenantId, primary)));
    }

    @Override
    public <T> T read(String tenantId, ReadPreference pref, Function<TagReader, T> work) {
        if (pref.snapshot()) {
            return snapshotTx.execute(status -> work.apply(new Reader(tenantId, primary)));
        }
        if (replica != null && pref.consistency() != Consistency.STRONG) {
            // Check and read in one replica transaction: behind a load-balanced reader endpoint, two
            // connections may land on two replicas with different lag.
            Object r = replicaTx.execute(status ->
                    replicaMayServe(pref) ? work.apply(new Reader(tenantId, replica)) : REPLICA_BEHIND);
            if (r != REPLICA_BEHIND) {
                @SuppressWarnings("unchecked") T value = (T) r;
                return value;
            }
        }
        return readTx.execute(status -> work.apply(new Reader(tenantId, primary)));
    }

    /**
     * A replica may serve EVENTUAL always, and SESSION once its copy of the relay position has reached
     * the token. Runs inside the replica read transaction, on the connection that will serve the read. That check is conservative and correct: the relay position only moves after every
     * event at or below it has committed on the primary, and a replica applies commits in order.
     */
    private boolean replicaMayServe(ReadPreference pref) {
        if (pref.consistency() == Consistency.EVENTUAL) {
            return true;
        }
        if (pref.consistency() == Consistency.SESSION) {
            Long relayed = replica.queryForObject(
                    "SELECT last_seq FROM outbox_relay_state WHERE id = 1", Map.of(), Long.class);
            return relayed != null && relayed >= pref.minSeq();
        }
        return false;
    }

    @Override
    public int relay(int maxEvents, Duration gapTimeout, Consumer<List<TagEvent>> sink) {
        Integer n = relayTx.execute(status -> {
            // One relay at a time per shard: the state row lock serializes relays across pods and keeps seq
            // order. SKIP LOCKED: a pod that loses the race returns at once instead of holding a pooled
            // connection while the winner publishes.
            List<Long> locked = primary.queryForList(
                    "SELECT last_seq FROM outbox_relay_state WHERE id = 1 FOR UPDATE SKIP LOCKED", Map.of(), Long.class);
            if (locked.isEmpty()) {
                return 0;
            }
            long position = locked.get(0) == null ? 0 : locked.get(0);
            List<OutboxRow> rows = primary.query(
                    "SELECT seq, payload, created_at FROM outbox WHERE seq > :last ORDER BY seq LIMIT :max",
                    new MapSqlParameterSource("last", position).addValue("max", maxEvents),
                    (rs, i) -> new OutboxRow(rs.getLong("seq"), rs.getString("payload"), instant(rs, "created_at")));
            Instant gapCutoff = clock.instant().minus(gapTimeout);
            List<TagEvent> batch = new ArrayList<>(rows.size());
            long expected = position + 1;
            for (OutboxRow row : rows) {
                if (row.seq != expected) {
                    if (row.createdAt.isAfter(gapCutoff)) {
                        break;  // a lower seq may still commit; wait for it (or for the gap to age out)
                    }
                    log.warn("shard {}: skipping outbox seqs {}..{} (rolled back or lost)", shardId, expected, row.seq - 1);
                }
                batch.add(decode(row.payload).sequenced(shardId, row.seq));
                expected = row.seq + 1;
            }
            if (batch.isEmpty()) {
                return 0;
            }
            sink.accept(batch);
            primary.update("UPDATE outbox_relay_state SET last_seq = :seq, updated_at = :now WHERE id = 1",
                    new MapSqlParameterSource("seq", expected - 1).addValue("now", now()));
            return batch.size();
        });
        return n == null ? 0 : n;
    }

    @Override
    public Optional<Instant> oldestUnrelayed() {
        // The outbox PK is seq, so this reads one index entry past the relay position.
        return primary.query("SELECT created_at FROM outbox WHERE seq > "
                        + "(SELECT last_seq FROM outbox_relay_state WHERE id = 1) ORDER BY seq LIMIT 1",
                Map.of(), (rs, i) -> instant(rs, "created_at")).stream().findFirst();
    }

    @Override
    public int pruneOutbox(Instant cutoff) {
        return primary.update("DELETE FROM outbox WHERE created_at < :cutoff AND seq <= "
                        + "(SELECT last_seq FROM outbox_relay_state WHERE id = 1)",
                new MapSqlParameterSource("cutoff", odt(cutoff)));
    }

    @Override
    public int pruneIdempotency(Instant cutoff) {
        return primary.update("DELETE FROM idempotency_keys WHERE created_at < :cutoff",
                new MapSqlParameterSource("cutoff", odt(cutoff)));
    }

    // ---------------------------------------------------------------------------------------------

    private record OutboxRow(long seq, String payload, Instant createdAt) {
    }

    private TagEvent decode(String payload) {
        try {
            return json.readValue(payload, TagEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt outbox payload", e);
        }
    }

    private String encode(TagEvent event) {
        try {
            return json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize event", e);
        }
    }

    private OffsetDateTime now() {
        return odt(clock.instant());
    }

    private static OffsetDateTime odt(Instant i) {
        return i == null ? null : i.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        OffsetDateTime v = rs.getObject(col, OffsetDateTime.class);
        return v == null ? null : v.toInstant();
    }

    private static final RowMapper<Tag> TAG = (rs, i) -> new Tag(
            rs.getString("tenant_id"), rs.getLong("tag_id"), rs.getString("name"), rs.getString("name_norm"),
            rs.getString("color"), rs.getLong("version"), rs.getBoolean("deleted"), rs.getString("created_by"),
            instant(rs, "created_at"), instant(rs, "updated_at"));

    private static final RowMapper<EntityHit> HIT = (rs, i) -> new EntityHit(
            rs.getLong("entity_seq"), new EntityRef(rs.getString("entity_type"), rs.getString("entity_id")));

    private static String likePrefix(String prefix) {
        return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%";
    }

    private static boolean isNameConflict(DuplicateKeyException e) {
        String msg = String.valueOf(e.getMostSpecificCause().getMessage()).toLowerCase(Locale.ROOT);
        return msg.contains("ux_tags_name");
    }

    // ---------------------------------------------------------------------------------------------

    /** A tenant-bound read view over one connection/transaction. */
    private class Reader implements TagReader {
        final String tenant;
        final NamedParameterJdbcTemplate jdbc;

        Reader(String tenant, NamedParameterJdbcTemplate jdbc) {
            this.tenant = Objects.requireNonNull(tenant, "tenant");
            this.jdbc = jdbc;
        }

        MapSqlParameterSource p() {
            return new MapSqlParameterSource("t", tenant);
        }

        @Override
        public Optional<Tag> findTag(long tagId) {
            return jdbc.query("SELECT * FROM tags WHERE tenant_id = :t AND tag_id = :id",
                    p().addValue("id", tagId), TAG).stream().findFirst();
        }

        @Override
        public Optional<Tag> findTagByName(String nameNorm) {
            return jdbc.query("SELECT * FROM tags WHERE tenant_id = :t AND name_norm = :n AND deleted = FALSE",
                    p().addValue("n", nameNorm), TAG).stream().findFirst();
        }

        @Override
        public List<Tag> findTags(Collection<Long> tagIds) {
            if (tagIds.isEmpty()) {
                return List.of();
            }
            return jdbc.query("SELECT * FROM tags WHERE tenant_id = :t AND tag_id IN (:ids) ORDER BY tag_id",
                    p().addValue("ids", new ArrayList<>(tagIds)), TAG);
        }

        @Override
        public Page<Tag> listTags(String prefixNorm, String afterNameNorm, int limit) {
            StringBuilder sql = new StringBuilder("SELECT * FROM tags WHERE tenant_id = :t AND deleted = FALSE");
            MapSqlParameterSource params = p().addValue("lim", limit + 1);
            if (prefixNorm != null && !prefixNorm.isEmpty()) {
                sql.append(" AND name_norm LIKE :prefix ESCAPE '\\'");
                params.addValue("prefix", likePrefix(prefixNorm));
            }
            if (afterNameNorm != null) {
                sql.append(" AND name_norm > :after");
                params.addValue("after", afterNameNorm);
            }
            sql.append(" ORDER BY name_norm LIMIT :lim");
            List<Tag> rows = jdbc.query(sql.toString(), params, TAG);
            return Page.fromRows(rows, limit, t -> Cursors.ofName(t.nameNorm()));
        }

        @Override
        public long countLiveTags() {
            Long n = jdbc.queryForObject("SELECT COUNT(*) FROM tags WHERE tenant_id = :t AND deleted = FALSE",
                    p(), Long.class);
            return n == null ? 0 : n;
        }

        @Override
        public Map<Long, Long> usage(Collection<Long> tagIds) {
            if (tagIds.isEmpty()) {
                return Map.of();
            }
            Map<Long, Long> out = new HashMap<>();
            jdbc.query("SELECT tag_id, SUM(cnt) AS total FROM tag_usage WHERE tenant_id = :t AND tag_id IN (:ids) "
                            + "GROUP BY tag_id", p().addValue("ids", new ArrayList<>(tagIds)),
                    rs -> {
                        out.put(rs.getLong("tag_id"), rs.getLong("total"));
                    });
            return out;
        }

        @Override
        public List<Long> tagsOf(EntityRef e) {
            return jdbc.queryForList("SELECT tag_id FROM entity_tags WHERE tenant_id = :t AND entity_type = :type "
                    + "AND entity_id = :id ORDER BY tag_id", entity(e), Long.class);
        }

        @Override
        public List<EntityHit> entitiesOf(long tagId, String entityType, long afterSeq, int limit) {
            StringBuilder sql = new StringBuilder("SELECT entity_seq, entity_type, entity_id FROM entity_tags "
                    + "WHERE tenant_id = :t AND tag_id = :tag AND entity_seq > :after");
            MapSqlParameterSource params = p().addValue("tag", tagId).addValue("after", afterSeq)
                    .addValue("lim", limit + 1);
            if (entityType != null) {
                sql.append(" AND entity_type = :type");
                params.addValue("type", entityType);
            }
            sql.append(" ORDER BY entity_seq LIMIT :lim");
            return jdbc.query(sql.toString(), params, HIT);
        }

        @Override
        public List<EntityHit> search(TagQuery q) {
            MapSqlParameterSource params = p().addValue("after", q.afterSeq()).addValue("lim", q.limit() + 1);
            StringBuilder sql = new StringBuilder("SELECT c.entity_seq, c.entity_type, c.entity_id FROM (");
            if (!q.all().isEmpty()) {
                sql.append("SELECT entity_seq, entity_type, entity_id FROM entity_tags WHERE tenant_id = :t "
                        + "AND tag_id IN (:all) AND entity_seq > :after "
                        + "GROUP BY entity_seq, entity_type, entity_id HAVING COUNT(*) = :allCount");
                params.addValue("all", new ArrayList<>(q.all())).addValue("allCount", q.all().size());
            } else {
                sql.append("SELECT DISTINCT entity_seq, entity_type, entity_id FROM entity_tags WHERE tenant_id = :t "
                        + "AND tag_id IN (:any) AND entity_seq > :after");
            }
            sql.append(") c WHERE 1 = 1");
            if (!q.any().isEmpty()) {
                params.addValue("any", new ArrayList<>(q.any()));
                if (!q.all().isEmpty()) {
                    sql.append(" AND EXISTS (SELECT 1 FROM entity_tags x WHERE x.tenant_id = :t "
                            + "AND x.entity_type = c.entity_type AND x.entity_id = c.entity_id AND x.tag_id IN (:any))");
                }
            }
            if (!q.none().isEmpty()) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM entity_tags y WHERE y.tenant_id = :t "
                        + "AND y.entity_type = c.entity_type AND y.entity_id = c.entity_id AND y.tag_id IN (:none))");
                params.addValue("none", new ArrayList<>(q.none()));
            }
            if (q.entityType() != null) {
                sql.append(" AND c.entity_type = :type");
                params.addValue("type", q.entityType());
            }
            sql.append(" ORDER BY c.entity_seq LIMIT :lim");
            return jdbc.query(sql.toString(), params, HIT);
        }

        @Override
        public Map<Long, EntityRef> resolveEntities(Collection<Long> entitySeqs) {
            if (entitySeqs.isEmpty()) {
                return Map.of();
            }
            Map<Long, EntityRef> out = new HashMap<>();
            for (EntityHit h : jdbc.query("SELECT entity_seq, entity_type, entity_id FROM entities "
                    + "WHERE tenant_id = :t AND entity_seq IN (:seqs)", p().addValue("seqs", new ArrayList<>(entitySeqs)), HIT)) {
                out.put(h.entitySeq(), h.entity());
            }
            return out;
        }

        @Override
        public long latestSeq() {   // index-only on ix_outbox_tenant_seq
            Long v = jdbc.queryForObject("SELECT MAX(seq) FROM outbox WHERE tenant_id = :t", p(), Long.class);
            return v == null ? 0 : v;
        }

        @Override
        public long relayedSeq() {
            Long v = jdbc.queryForObject("SELECT last_seq FROM outbox_relay_state WHERE id = 1", Map.of(), Long.class);
            return v == null ? 0 : v;
        }

        @Override
        public void scanAssignments(Consumer<Posting> sink) {
            // Plain JdbcTemplate with a fetch size: inside a transaction, PostgreSQL streams with a cursor.
            scanJdbc.query("SELECT et.tag_id, et.entity_type, et.entity_seq FROM entity_tags et "
                            + "JOIN tags t ON t.tenant_id = et.tenant_id AND t.tag_id = et.tag_id "
                            + "WHERE et.tenant_id = ? AND t.deleted = FALSE",
                    rs -> {
                        sink.accept(new Posting(rs.getLong(1), rs.getString(2), rs.getLong(3)));
                    }, tenant);
        }

        MapSqlParameterSource entity(EntityRef e) {
            return p().addValue("type", e.type()).addValue("id", e.id());
        }
    }

    /** A tenant-bound unit of work inside the surrounding write transaction. */
    private final class Uow extends Reader implements UnitOfWork {
        Uow(String tenant, NamedParameterJdbcTemplate jdbc) {
            super(tenant, jdbc);
        }

        @Override
        public Tag insertTag(Tag tag) {
            requireTenant(tag.tenantId());
            try {
                jdbc.update("INSERT INTO tags (tenant_id, tag_id, name, name_norm, color, version, deleted, "
                        + "created_by, created_at, updated_at) VALUES (:t, :id, :name, :norm, :color, :v, :del, "
                        + ":by, :ca, :ua)", tagParams(tag).addValue("v", tag.version()));
                return tag;
            } catch (DuplicateKeyException e) {
                if (isNameConflict(e)) {
                    throw new DuplicateTagNameException("a tag named '" + tag.name() + "' already exists");
                }
                throw e;
            }
        }

        @Override
        public Tag updateTag(Tag tag, long expectedVersion) {
            requireTenant(tag.tenantId());
            int n;
            try {
                n = jdbc.update("UPDATE tags SET name = :name, name_norm = :norm, color = :color, deleted = :del, "
                                + "updated_at = :ua, version = version + 1 "
                                + "WHERE tenant_id = :t AND tag_id = :id AND version = :expected",
                        tagParams(tag).addValue("expected", expectedVersion));
            } catch (DuplicateKeyException e) {
                if (isNameConflict(e)) {
                    throw new DuplicateTagNameException("a tag named '" + tag.name() + "' already exists");
                }
                throw e;
            }
            if (n == 0) {
                throw new StaleVersionException("tag " + tag.tagId() + " is not at version " + expectedVersion);
            }
            return tag.withVersion(expectedVersion + 1);
        }

        @Override
        public long entitySeq(EntityRef e, boolean create) {
            // FOR UPDATE: all writes to one entity serialize on its row, so multi-statement changes
            // (replace = detach + attach) can't deadlock and per-entity limits are exact.
            String select = "SELECT entity_seq FROM entities WHERE tenant_id = :t AND entity_type = :type "
                    + "AND entity_id = :id FOR UPDATE";
            List<Long> found = jdbc.queryForList(select, entity(e), Long.class);
            if (!found.isEmpty()) {
                return found.get(0);
            }
            if (!create) {
                return 0;
            }
            jdbc.update("INSERT INTO entities (tenant_id, entity_type, entity_id) VALUES (:t, :type, :id) "
                    + "ON CONFLICT DO NOTHING", entity(e));
            Long seq = jdbc.queryForObject(select, entity(e), Long.class);
            return Objects.requireNonNull(seq, "entity seq");
        }

        @Override
        public Set<Long> attach(EntityRef e, long entitySeq, Collection<Long> tagIds, String actor, Instant at) {
            List<Long> ids = new ArrayList<>(new TreeSet<>(tagIds));    // fixed lock order: no deadlocks
            if (ids.isEmpty()) {
                return Set.of();
            }
            SqlParameterSource[] batch = new SqlParameterSource[ids.size()];
            for (int i = 0; i < ids.size(); i++) {
                batch[i] = entity(e).addValue("tag", ids.get(i)).addValue("seq", entitySeq)
                        .addValue("by", actor).addValue("at", odt(at));
            }
            int[] counts = jdbc.batchUpdate("INSERT INTO entity_tags (tenant_id, entity_type, entity_id, tag_id, "
                    + "entity_seq, created_by, created_at) VALUES (:t, :type, :id, :tag, :seq, :by, :at) "
                    + "ON CONFLICT DO NOTHING", batch);
            return changed(ids, counts);
        }

        @Override
        public Set<Long> detach(EntityRef e, Collection<Long> tagIds) {
            List<Long> ids = new ArrayList<>(new TreeSet<>(tagIds));
            if (ids.isEmpty()) {
                return Set.of();
            }
            SqlParameterSource[] batch = new SqlParameterSource[ids.size()];
            for (int i = 0; i < ids.size(); i++) {
                batch[i] = entity(e).addValue("tag", ids.get(i));
            }
            int[] counts = jdbc.batchUpdate("DELETE FROM entity_tags WHERE tenant_id = :t AND entity_type = :type "
                    + "AND entity_id = :id AND tag_id = :tag", batch);
            return changed(ids, counts);
        }

        private Set<Long> changed(List<Long> ids, int[] counts) {
            Set<Long> out = new LinkedHashSet<>();
            for (int i = 0; i < ids.size(); i++) {
                if (counts[i] == Statement.SUCCESS_NO_INFO) {
                    throw new IllegalStateException("JDBC driver returned SUCCESS_NO_INFO; disable batch rewriting");
                }
                if (counts[i] > 0) {
                    out.add(ids.get(i));
                }
            }
            return out;
        }

        @Override
        public void adjustUsage(Map<Long, Long> deltas) {
            for (Map.Entry<Long, Long> d : new TreeMap<>(deltas).entrySet()) {
                if (d.getValue() == 0) {
                    continue;
                }
                MapSqlParameterSource params = p().addValue("tag", d.getKey()).addValue("d", d.getValue())
                        .addValue("b", ThreadLocalRandom.current().nextInt(usageBuckets));
                String update = "UPDATE tag_usage SET cnt = cnt + :d WHERE tenant_id = :t AND tag_id = :tag AND bucket = :b";
                if (jdbc.update(update, params) == 0
                        && jdbc.update("INSERT INTO tag_usage (tenant_id, tag_id, bucket, cnt) VALUES (:t, :tag, :b, :d) "
                        + "ON CONFLICT DO NOTHING", params) == 0) {
                    jdbc.update(update, params);   // lost the insert race: the row exists now
                }
            }
        }

        @Override
        public int purgeAssignments(long tagId, int limit) {
            List<EntityRef> victims = jdbc.query("SELECT entity_type, entity_id FROM entity_tags WHERE tenant_id = :t "
                            + "AND tag_id = :tag ORDER BY entity_seq LIMIT :lim",
                    p().addValue("tag", tagId).addValue("lim", limit),
                    (rs, i) -> new EntityRef(rs.getString(1), rs.getString(2)));
            if (!victims.isEmpty()) {
                SqlParameterSource[] batch = victims.stream()
                        .map(v -> entity(v).addValue("tag", tagId)).toArray(SqlParameterSource[]::new);
                jdbc.batchUpdate("DELETE FROM entity_tags WHERE tenant_id = :t AND entity_type = :type "
                        + "AND entity_id = :id AND tag_id = :tag", batch);
            }
            if (victims.size() < limit) {
                jdbc.update("DELETE FROM tag_usage WHERE tenant_id = :t AND tag_id = :tag", p().addValue("tag", tagId));
            }
            return victims.size();
        }

        @Override
        public long append(TagEvent event) {
            requireTenant(event.tenantId());
            GeneratedKeyHolder keys = new GeneratedKeyHolder();
            jdbc.update("INSERT INTO outbox (tenant_id, event_type, payload, created_at) VALUES (:t, :type, :payload, :at)",
                    p().addValue("type", event.type().name()).addValue("payload", encode(event)).addValue("at", now()),
                    keys, new String[]{"seq"});
            Number key = keys.getKey();
            return Objects.requireNonNull(key, "outbox seq").longValue();
        }

        @Override
        public Optional<IdempotencyRecord> findIdempotency(String key) {
            return jdbc.query("SELECT * FROM idempotency_keys WHERE tenant_id = :t AND idem_key = :k",
                    p().addValue("k", key), (rs, i) -> new IdempotencyRecord(rs.getString("tenant_id"),
                            rs.getString("idem_key"), rs.getString("request_hash"), rs.getString("response"),
                            instant(rs, "created_at"))).stream().findFirst();
        }

        @Override
        public void saveIdempotency(IdempotencyRecord r) {
            requireTenant(r.tenantId());
            jdbc.update("INSERT INTO idempotency_keys (tenant_id, idem_key, request_hash, response, created_at) "
                            + "VALUES (:t, :k, :h, :r, :at)",
                    p().addValue("k", r.key()).addValue("h", r.requestHash()).addValue("r", r.response())
                            .addValue("at", odt(r.createdAt())));
        }

        @Override
        public void completeIdempotency(String key, String response) {
            int n = jdbc.update("UPDATE idempotency_keys SET response = :r WHERE tenant_id = :t AND idem_key = :k",
                    p().addValue("k", key).addValue("r", response));
            if (n != 1) {
                throw new IllegalStateException("idempotency key " + key + " was not claimed");
            }
        }

        private MapSqlParameterSource tagParams(Tag tag) {
            return p().addValue("id", tag.tagId()).addValue("name", tag.name()).addValue("norm", tag.nameNorm())
                    .addValue("color", tag.color()).addValue("del", tag.deleted()).addValue("by", tag.createdBy())
                    .addValue("ca", odt(tag.createdAt())).addValue("ua", odt(tag.updatedAt()));
        }

        private void requireTenant(String t) {
            if (!tenant.equals(t)) {
                throw new IllegalArgumentException("unit of work is bound to another tenant");
            }
        }
    }
}
