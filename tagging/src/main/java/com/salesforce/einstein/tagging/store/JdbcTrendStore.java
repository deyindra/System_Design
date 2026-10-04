package com.salesforce.einstein.tagging.store;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import com.salesforce.einstein.tagging.spi.TrendStore;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@link TrendStore} over JDBC (PostgreSQL, or H2 in PostgreSQL mode). Same portable SQL as
 * {@link JdbcTagStore}: {@code ON CONFLICT DO NOTHING}, {@code FOR UPDATE}, no vendor upsert.
 *
 * <p>Exactly-once: one transaction per delivered batch locks each tenant's {@code trend_progress} row
 * (in tenant order, so concurrent consumers can't deadlock), skips events at or below its
 * {@code last_seq}, adds the rest to {@code tag_activity} and advances {@code last_seq}. Holding the
 * progress row means no one else writes that tenant's counters meanwhile, so the upsert can't race.
 *
 * <p>Takes ownership of the data source: {@link #close()} closes it if it is closeable (a pool).
 */
@SuppressWarnings("SqlNoDataSourceInspection")   // IntelliJ: the SQL is checked by the H2 tests, not an IDE data source
public final class JdbcTrendStore implements TrendStore, AutoCloseable {
    private final DataSource ds;
    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Clock clock;

    public JdbcTrendStore(DataSource ds, Clock clock) {
        this.ds = Objects.requireNonNull(ds, "ds");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.jdbc = new NamedParameterJdbcTemplate(ds);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        this.tx.setTimeout(30);
    }

    /** A counter row's key. */
    private record Cell(int grain, long bucket, long tagId) {
    }

    @Override
    public void record(String sourceShard, List<TagEvent> events) {
        Map<String, List<TagEvent>> byTenant = new TreeMap<>();   // lock order
        for (TagEvent e : events) {
            if (e.type() == TagEvent.Type.TAGS_ATTACHED || e.type() == TagEvent.Type.TAG_DELETED) {
                byTenant.computeIfAbsent(e.tenantId(), k -> new ArrayList<>()).add(e);
            }
        }
        if (byTenant.isEmpty()) {
            return;
        }
        tx.executeWithoutResult(status -> byTenant.forEach((tenant, list) -> apply(sourceShard, tenant, list)));
    }

    private void apply(String shard, String tenant, List<TagEvent> events) {
        MapSqlParameterSource key = new MapSqlParameterSource("t", tenant).addValue("s", shard)
                .addValue("now", clock.instant().atOffset(ZoneOffset.UTC));
        jdbc.update("INSERT INTO trend_progress (tenant_id, source_shard, last_seq, updated_at) "
                + "VALUES (:t, :s, 0, :now) ON CONFLICT DO NOTHING", key);
        long last = jdbc.queryForObject("SELECT last_seq FROM trend_progress WHERE tenant_id = :t AND source_shard = :s "
                + "FOR UPDATE", key, Long.class);

        TreeMap<Cell, Long> deltas = new TreeMap<>(Comparator.comparingInt(Cell::grain)
                .thenComparingLong(Cell::bucket).thenComparingLong(Cell::tagId));
        Set<Long> deleted = new HashSet<>();
        long high = last;
        for (TagEvent e : events) {
            if (e.seq() <= last) {
                continue;   // already counted: a redelivery
            }
            high = Math.max(high, e.seq());
            if (e.type() == TagEvent.Type.TAG_DELETED) {
                deleted.add(e.tagId());
                continue;
            }
            for (int grain : TrendWindow.GRAINS) {
                long bucket = TrendWindow.bucket(e.at(), grain);
                for (long tagId : e.tagIds()) {
                    deltas.merge(new Cell(grain, bucket, tagId), 1L, Long::sum);
                }
            }
        }
        if (high == last) {
            return;
        }
        // A deleted tag can't be attached afterwards, so all of its pending deltas precede the delete.
        deltas.keySet().removeIf(c -> deleted.contains(c.tagId()));
        upsert(tenant, deltas);
        for (long tagId : deleted) {
            jdbc.update("DELETE FROM tag_activity WHERE tenant_id = :t AND tag_id = :tag",
                    new MapSqlParameterSource("t", tenant).addValue("tag", tagId));
        }
        jdbc.update("UPDATE trend_progress SET last_seq = :seq, updated_at = :now WHERE tenant_id = :t "
                + "AND source_shard = :s", key.addValue("seq", high));
    }

    private void upsert(String tenant, TreeMap<Cell, Long> deltas) {
        if (deltas.isEmpty()) {
            return;
        }
        List<SqlParameterSource> rows = new ArrayList<>(deltas.size());
        deltas.forEach((c, d) -> rows.add(new MapSqlParameterSource("t", tenant).addValue("g", c.grain())
                .addValue("b", c.bucket()).addValue("tag", c.tagId()).addValue("d", d)));
        int[] updated = jdbc.batchUpdate("UPDATE tag_activity SET attaches = attaches + :d WHERE tenant_id = :t "
                + "AND grain = :g AND bucket = :b AND tag_id = :tag", rows.toArray(SqlParameterSource[]::new));
        List<SqlParameterSource> missing = new ArrayList<>();
        for (int i = 0; i < updated.length; i++) {
            if (updated[i] == 0) {
                missing.add(rows.get(i));
            } else if (updated[i] != 1) {
                throw new IllegalStateException("unexpected batch update count " + updated[i]
                        + " (is reWriteBatchedInserts on?)");
            }
        }
        if (!missing.isEmpty()) {
            // No race: this transaction holds the tenant's progress row, the only path that writes its counters.
            jdbc.batchUpdate("INSERT INTO tag_activity (tenant_id, grain, bucket, tag_id, attaches) "
                    + "VALUES (:t, :g, :b, :tag, :d)", missing.toArray(SqlParameterSource[]::new));
        }
    }

    @Override
    public List<TagCount> top(String tenantId, int grainSeconds, long firstBucket, long lastBucket, TrendRank rank,
                              int limit) {
        String cur = "SUM(CASE WHEN bucket >= :first THEN attaches ELSE 0 END)";
        String prev = "SUM(CASE WHEN bucket < :first THEN attaches ELSE 0 END)";
        String filterAndOrder = switch (rank) {
            case POPULAR -> " HAVING " + cur + " > 0 ORDER BY " + cur + " DESC, tag_id";
            case RISING -> " HAVING " + cur + " > " + prev + " ORDER BY " + cur + " - " + prev + " DESC, "
                    + cur + " DESC, tag_id";
        };
        long span = lastBucket - firstBucket + 1;
        return jdbc.query("SELECT tag_id, " + cur + " AS cnt, " + prev + " AS prev FROM tag_activity "
                        + "WHERE tenant_id = :t AND grain = :g AND bucket BETWEEN :from AND :last "
                        + "GROUP BY tag_id" + filterAndOrder + " LIMIT :lim",
                new MapSqlParameterSource("t", tenantId).addValue("g", grainSeconds).addValue("first", firstBucket)
                        .addValue("from", firstBucket - span).addValue("last", lastBucket).addValue("lim", limit),
                (rs, i) -> new TagCount(rs.getLong("tag_id"), rs.getLong("cnt"), rs.getLong("prev")));
    }

    @Override
    public int prune(int grainSeconds, long beforeBucket) {
        return jdbc.update("DELETE FROM tag_activity WHERE grain = :g AND bucket < :b",
                new MapSqlParameterSource("g", grainSeconds).addValue("b", beforeBucket));
    }

    @Override
    public void close() throws Exception {
        if (ds instanceof AutoCloseable c) {
            c.close();
        }
    }
}
