package com.salesforce.einstein.tagging.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.salesforce.einstein.tagging.cache.NoopDistributedCache;
import com.salesforce.einstein.tagging.cache.TagCache;
import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.EntityTags;
import com.salesforce.einstein.tagging.domain.IdempotencyConflictException;
import com.salesforce.einstein.tagging.domain.LimitExceededException;
import com.salesforce.einstein.tagging.domain.NotFoundException;
import com.salesforce.einstein.tagging.domain.SearchResult;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.WriteResult;
import com.salesforce.einstein.tagging.events.InProcessEventPublisher;
import com.salesforce.einstein.tagging.events.OutboxRelay;
import com.salesforce.einstein.tagging.index.RoaringInvertedIndex;
import com.salesforce.einstein.tagging.service.AssignmentService.BulkAttachResult;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.store.H2;
import com.salesforce.einstein.tagging.store.JdbcTagStore;
import com.salesforce.einstein.tagging.tenant.Resilience4jTenantIsolation;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import com.salesforce.einstein.tagging.tenant.SnowflakeIdGenerator;
import com.salesforce.einstein.tagging.tenant.StaticTenantDirectory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The services over a real (H2, PostgreSQL mode) shard, with the Roaring index fed by the relay. */
class AssignmentServiceTest {
    private static final String T1 = "tenant-1";
    private static final String T2 = "tenant-2";
    private static final EntityRef ISSUE = new EntityRef("jira:issue", "10042");

    private final ExecutorService purgeExecutor = Executors.newSingleThreadExecutor();
    private TagService tags;
    private AssignmentService assignments;
    private TagSearchService search;
    private OutboxRelay relay;
    private TagStore store;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.systemUTC();
        ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
        TaggingProperties props = TaggingProperties.of(Map.of("limits.max-tags-per-entity", "5"));
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        store = new JdbcTagStore("s0", H2.newDatabase(), null, json, clock, 4);
        ShardRouter router = new ShardRouter(new StaticTenantDirectory(props), Map.of("s0", store));
        var isolation = new Resilience4jTenantIsolation(props, metrics);
        var ids = new SnowflakeIdGenerator(1, clock);
        TagCache cache = new TagCache(props.cache(), new NoopDistributedCache());
        RoaringInvertedIndex index = new RoaringInvertedIndex(router::store, 100, Runnable::run);
        TagPurgeJob purge = new TagPurgeJob(router, purgeExecutor, 100, metrics);

        tags = new TagService(router, isolation, ids, cache, props.limits(), clock);
        assignments = new AssignmentService(router, isolation, cache, ids, new IdempotencyService(json, clock),
                props.limits(), clock);
        search = new TagSearchService(router, isolation, index, props, metrics);
        relay = new OutboxRelay(List.of(store), new InProcessEventPublisher(List.of(index, cache, purge)),
                props.relay(), clock, metrics);
    }

    @AfterEach
    void tearDown() {
        purgeExecutor.shutdownNow();
    }

    /** Creates a tag in {@link #T1}, the tenant every test writes to. */
    private long tag(String name) {
        return tags.create(T1, "alice", name, null).value().tagId();
    }

    private static List<String> names(EntityTags et) {
        return et.tags().stream().map(Tag::name).toList();
    }

    @Test
    @DisplayName("attach by name creates missing tags; repeating it changes nothing")
    void attachByNameIdempotent() {
        long bug = tag("bug");
        WriteResult<EntityTags> first = assignments.attach(T1, "alice", ISSUE, List.of(bug), List.of("Urgent", "BUG"));
        assertThat(names(first.value())).containsExactly("bug", "Urgent");
        assertThat(first.token().isNone()).isFalse();

        WriteResult<EntityTags> again = assignments.attach(T1, "alice", ISSUE, null, List.of("urgent"));
        assertThat(again.token().isNone()).as("no change, no event").isTrue();
        assertThat(names(again.value())).containsExactly("bug", "Urgent");
        assertThat(tags.get(T1, bug, Consistency.STRONG, null).usage()).isEqualTo(1);
    }

    @Test
    @DisplayName("unknown, deleted and other tenants' tags are rejected")
    void rejectsUnknownTags() {
        long t1Tag = tag("secret");
        assertThatThrownBy(() -> assignments.attach(T2, "bob", ISSUE, List.of(t1Tag), null))
                .isInstanceOf(NotFoundException.class);
        tags.delete(T1, "alice", t1Tag, null);
        assertThatThrownBy(() -> assignments.attach(T1, "alice", ISSUE, List.of(t1Tag), null))
                .isInstanceOf(NotFoundException.class);
    }

    @Test
    @DisplayName("replace sets exactly the given tags and fixes usage counts")
    void replace() {
        long a = tag("a");
        long b = tag("b");
        long c = tag("c");
        assignments.attach(T1, "u", ISSUE, List.of(a, b), null);
        EntityTags after = assignments.replace(T1, "u", ISSUE, List.of(b, c)).value();
        assertThat(names(after)).containsExactly("b", "c");
        assertThat(tags.get(T1, a, Consistency.STRONG, null).usage()).isZero();
        assertThat(tags.get(T1, b, Consistency.STRONG, null).usage()).isEqualTo(1);
        assertThat(names(assignments.replace(T1, "u", ISSUE, List.of()).value())).isEmpty();
    }

    @Test
    @DisplayName("the per-entity limit holds exactly under concurrent attaches")
    void perEntityLimitUnderConcurrency() throws Exception {
        long first = tag("t0");
        assignments.attach(T1, "u", ISSUE, List.of(first), null);
        List<Long> candidates = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            candidates.add(tag("t" + i));
        }
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        for (Long id : candidates) {
            results.add(pool.submit(() -> {
                start.await();
                try {
                    assignments.attach(T1, "u", ISSUE, List.of(id), null);
                    return true;
                } catch (LimitExceededException e) {
                    return false;
                }
            }));
        }
        start.countDown();
        int ok = 0;
        for (Future<Boolean> f : results) {
            ok += f.get(30, TimeUnit.SECONDS) ? 1 : 0;
        }
        pool.shutdown();
        assertThat(ok).isEqualTo(4);   // 1 + 4 = the limit of 5
        assertThat(assignments.tagsOf(T1, ISSUE, Consistency.STRONG, null).tags()).hasSize(5);
    }

    @Test
    @DisplayName("bulk attach replays on a retried key and rejects a reused key with another body")
    void bulkIdempotency() {
        long a = tag("a");
        List<EntityRef> entities = List.of(ISSUE, new EntityRef("confluence:page", "7"));
        WriteResult<BulkAttachResult> r1 = assignments.bulkAttach(T1, "u", "import-0001", entities, List.of(a));
        WriteResult<BulkAttachResult> r2 = assignments.bulkAttach(T1, "u", "import-0001", List.of(entities.get(1), entities.get(0)), List.of(a));
        assertThat(r1.value()).isEqualTo(new BulkAttachResult(2, 2));
        assertThat(r2).isEqualTo(r1);
        assertThat(tags.get(T1, a, Consistency.STRONG, null).usage()).isEqualTo(2);
        assertThatThrownBy(() -> assignments.bulkAttach(T1, "u", "import-0001", List.of(ISSUE), List.of(a)))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    @DisplayName("a session read sees the caller's write even when a cached copy exists")
    void readYourWrites() {
        long a = tag("a");
        assertThat(assignments.tagsOf(T1, ISSUE, Consistency.EVENTUAL, null).tags()).isEmpty();   // cached
        ConsistencyToken token = assignments.attach(T1, "u", ISSUE, List.of(a), null).token();
        assertThat(names(assignments.tagsOf(T1, ISSUE, Consistency.SESSION, token))).containsExactly("a");
    }

    @Test
    @DisplayName("search is served by the index once caught up, and by the store before that")
    void searchConsistency() {
        long a = tag("a");
        long b = tag("b");
        assignments.attach(T1, "u", ISSUE, List.of(a, b), null);
        relay.relayAll();
        // first search starts the bootstrap (synchronous here)
        search.search(T1, List.of(a), null, null, null, null, 10, Consistency.EVENTUAL, null);

        ConsistencyToken token = assignments.attach(T1, "u", new EntityRef("jira:issue", "2"), List.of(a), null).token();
        SearchResult behind = search.search(T1, List.of(a), null, List.of(b), null, null, 10, Consistency.SESSION, token);
        assertThat(behind.servedBy()).isEqualTo("store");   // index hasn't applied that attach yet
        assertThat(behind.items()).containsExactly(new EntityRef("jira:issue", "2"));

        relay.relayAll();
        SearchResult caughtUp = search.search(T1, List.of(a), null, List.of(b), null, null, 10, Consistency.SESSION, token);
        assertThat(caughtUp.servedBy()).isEqualTo("index");
        assertThat(caughtUp.items()).isEqualTo(behind.items());
        assertThat(caughtUp.total()).isEqualTo(1);
    }

    @Test
    @DisplayName("a strong search uses the index only once it has reached the primary's read barrier")
    void strongSearchBarrier() {
        long a = tag("a");
        assignments.attach(T1, "u", ISSUE, List.of(a), null);
        relay.relayAll();
        search.search(T1, List.of(a), null, null, null, null, 10, null, null);   // starts the bootstrap

        assignments.attach(T1, "u", new EntityRef("jira:issue", "2"), List.of(a), null);   // acknowledged, not relayed
        SearchResult behind = search.search(T1, List.of(a), null, null, null, null, 10, null, null);   // default: STRONG
        assertThat(behind.servedBy()).isEqualTo("store");
        assertThat(behind.items()).hasSize(2);

        relay.relayAll();
        SearchResult caughtUp = search.search(T1, List.of(a), null, null, null, null, 10, Consistency.STRONG, null);
        assertThat(caughtUp.servedBy()).isEqualTo("index");
        assertThat(caughtUp.items()).isEqualTo(behind.items());
    }

    @Test
    @DisplayName("deleting a tag hides it at once and purges its assignments")
    void deletePurges() throws Exception {
        long a = tag("a");
        long b = tag("b");
        assignments.attach(T1, "u", ISSUE, List.of(a, b), null);
        tags.delete(T1, "u", a, null);
        assertThat(names(assignments.tagsOf(T1, ISSUE, Consistency.STRONG, null))).containsExactly("b");
        relay.relayAll();   // TAG_DELETED reaches the purge job
        purgeExecutor.submit(() -> null).get(10, TimeUnit.SECONDS);
        assertThat(store.read(T1, com.salesforce.einstein.tagging.spi.ReadPreference.strong(),
                (com.salesforce.einstein.tagging.spi.TagReader r) -> r.tagsOf(ISSUE)))
                .containsExactly(b);
    }
}
