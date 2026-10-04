package com.salesforce.einstein.tagging.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TagStoreContractTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("SqlNoDataSourceInspection")   // IntelliJ: the SQL runs against the H2 schema in these tests, not an IDE data source
class JdbcTagStoreTest extends TagStoreContractTest {
    private DataSource ds;
    private final MutableClock clock = new MutableClock(NOW);

    @Override
    protected TagStore newStore() {
        ds = H2.newDatabase();
        return new JdbcTagStore("s0", ds, null, mapper(), clock, 8);
    }

    static ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Test
    @DisplayName("relay stops at a young seq gap and skips it once it ages past the gap timeout")
    void relayWaitsOnGap() {
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        String payload = "{\"tenantId\":\"tenant-1\",\"type\":\"TAG_CREATED\",\"tagId\":1,\"tagIds\":[],"
                + "\"at\":\"2026-01-01T00:00:00Z\"}";
        jdbc.update("INSERT INTO outbox (seq, tenant_id, event_type, payload, created_at) VALUES (1, 'tenant-1', "
                + "'TAG_CREATED', ?, ?)", payload, NOW.atOffset(ZoneOffset.UTC));
        // seq 2 is "in flight" (never committed); seq 3 committed
        jdbc.update("INSERT INTO outbox (seq, tenant_id, event_type, payload, created_at) VALUES (3, 'tenant-1', "
                + "'TAG_CREATED', ?, ?)", payload, NOW.atOffset(ZoneOffset.UTC));

        List<TagEvent> out = new ArrayList<>();
        store.relay(10, GAP, out::addAll);
        assertThat(out).extracting(TagEvent::seq).containsExactly(1L);

        clock.advance(GAP.plusSeconds(1));
        store.relay(10, GAP, out::addAll);
        assertThat(out).extracting(TagEvent::seq).containsExactly(1L, 3L);
    }

    @Test
    @DisplayName("a second relay returns at once while another holds the shard's relay lock")
    void relayIsSingleActive() {
        Tag t = newTag(T1, "a");
        store.write(T1, uow -> uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T1, t.tagId(), "alice", NOW)));
        ExecutorService other = Executors.newSingleThreadExecutor();
        try {
            int[] concurrent = {-1};
            store.relay(10, GAP, batch -> {
                try {   // still inside the first relay's transaction, holding the lock
                    concurrent[0] = other.submit(() -> store.relay(10, GAP, b -> { })).get(5, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    throw new AssertionError(ex);
                }
            });
            assertThat(concurrent[0]).isZero();
        } finally {
            other.shutdownNow();
        }
    }

    @Test
    @DisplayName("concurrent attaches of the same (entity, tag) report exactly one addition")
    void concurrentAttach() throws Exception {
        Tag t = newTag(T1, "hot");
        EntityRef e = new EntityRef("jira:issue", "42");
        attach(T1, e);  // create the entity
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        Set<Integer> winners = ConcurrentHashMap.newKeySet();
        List<Future<?>> fs = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int id = i;
            fs.add(pool.submit(() -> {
                start.await();
                Set<Long> added = store.write(T1, uow -> uow.attach(e, uow.entitySeq(e, true), List.of(t.tagId()), "u", NOW));
                if (!added.isEmpty()) {
                    winners.add(id);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : fs) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertThat(winners).hasSize(1);
    }

    static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        MutableClock(Instant now) {
            this.now = new AtomicReference<>(now);
        }

        void advance(Duration d) {
            now.updateAndGet(t -> t.plus(d));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
