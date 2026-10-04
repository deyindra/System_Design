package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagQuery;
import com.salesforce.einstein.tagging.store.InMemoryTagStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The {@link TagSearchIndex} contract. The store is the source of truth: after a bootstrap plus any
 * prefix of the event stream, the index must answer exactly what the store's SQL path answers.
 */
public abstract class TagSearchIndexContractTest {
    protected static final String T1 = "tenant-1";
    protected static final String T2 = "tenant-2";
    protected static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    protected static final EntityRef ISSUE_1 = new EntityRef("jira:issue", "1");
    protected static final EntityRef ISSUE_2 = new EntityRef("jira:issue", "2");
    protected static final EntityRef PAGE_1 = new EntityRef("confluence:page", "1");

    private final AtomicLong ids = new AtomicLong(100);
    protected TagStore store;
    protected TagSearchIndex index;

    /** A fresh index that bootstraps tenants from {@code storeOf}. */
    protected abstract TagSearchIndex newIndex(Function<String, TagStore> storeOf);

    @BeforeEach
    void setUp() {
        store = new InMemoryTagStore("s0", Clock.fixed(NOW, ZoneOffset.UTC));
        index = newIndex(t -> store);
    }

    /** Creates a tag in {@link #T1}. */
    protected long tag(String name) {
        Tag t = Tag.create(T1, ids.incrementAndGet(), name, null, "u", NOW);
        store.write(T1, (UnitOfWork uow) -> {
            uow.insertTag(t);
            return uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T1, t.tagId(), "alice", NOW));
        });
        return t.tagId();
    }

    /** Attaches and appends the event, like the service does. Returns the entity seq. */
    protected long attach(EntityRef e, Long... tagIds) {
        return store.write(T1, (UnitOfWork uow) -> {
            long seq = uow.entitySeq(e, true);
            Set<Long> added = uow.attach(e, seq, List.of(tagIds), "u", NOW);
            uow.append(TagEvent.assignment(TagEvent.Type.TAGS_ATTACHED, T1, e, seq, List.copyOf(added), "alice", NOW));
            return seq;
        });
    }

    protected void detach(EntityRef e, Long... tagIds) {
        store.write(T1, (UnitOfWork uow) -> {
            long seq = uow.entitySeq(e, false);
            Set<Long> removed = uow.detach(e, List.of(tagIds));
            return uow.append(TagEvent.assignment(TagEvent.Type.TAGS_DETACHED, T1, e, seq, List.copyOf(removed), "alice", NOW));
        });
    }

    /** Soft-deletes a tag in {@link #T1}. */
    protected void deleteTag(long tagId) {
        store.write(T1, (UnitOfWork uow) -> {
            Tag t = uow.findTag(tagId).orElseThrow();
            uow.updateTag(t.tombstoned(NOW), t.version());
            return uow.append(TagEvent.tag(TagEvent.Type.TAG_DELETED, T1, tagId, "alice", NOW));
        });
    }

    /** Relays everything to the index and returns the delivered events. */
    protected List<TagEvent> relay() {
        List<TagEvent> all = new ArrayList<>();
        int relayed;
        do {
            relayed = store.relay(100, Duration.ofSeconds(10), batch -> {
                all.addAll(batch);
                index.onEvents(batch);
            });
        } while (relayed > 0);
        return all;
    }

    protected void ready(String tenant) {
        index.search(tenant, query(Set.of(1L), null, null, null));
        await().atMost(Duration.ofSeconds(5)).until(() -> index.watermark(tenant) >= 0);
    }

    protected static TagQuery query(Set<Long> all, Set<Long> any, Set<Long> none, String type) {
        return new TagQuery(all, any, none, type, 0, 100);
    }

    protected List<Long> hits(String tenant, TagQuery q) {
        Optional<SearchPostings> p = index.search(tenant, q);
        assertThat(p).as("index answers once ready").isPresent();
        return p.get().entitySeqs();
    }

    @Test
    @DisplayName("an unloaded tenant is not served, then bootstraps from a store snapshot")
    void bootstrap() {
        long a = tag("a");
        long b = tag("b");
        long s1 = attach(ISSUE_1, a, b);
        long s2 = attach(ISSUE_2, a);
        relay();
        assertThat(index.watermark(T1)).isEqualTo(-1);
        ready(T1);
        assertThat(index.watermark(T1)).isEqualTo(store.read(T1, ReadPreference.strong(), TagReader::relayedSeq));
        assertThat(hits(T1, query(Set.of(a), null, null, null))).containsExactly(s1, s2);
        assertThat(hits(T1, query(Set.of(a, b), null, null, null))).containsExactly(s1);
    }

    @Test
    @DisplayName("AND / OR / AND NOT and the type filter match the store")
    void booleanOps() {
        long a = tag("a");
        long b = tag("b");
        long c = tag("c");
        ready(T1);
        long s1 = attach(ISSUE_1, a, b);
        long s2 = attach(ISSUE_2, a, c);
        long s3 = attach(PAGE_1, b);
        relay();
        for (TagQuery q : List.of(
                query(Set.of(a), null, null, null),
                query(null, Set.of(b, c), null, null),
                query(Set.of(a), null, Set.of(c), null),
                query(Set.of(a), Set.of(c), null, null),
                query(null, Set.of(b), null, "confluence:page"),
                query(null, Set.of(a, b), Set.of(c), "jira:issue"))) {
            List<Long> fromStore = store.read(T1, ReadPreference.strong(), (TagReader r) -> r.search(q)).stream()
                    .map(EntityHit::entitySeq).toList();
            assertThat(hits(T1, q)).as(q.toString()).isEqualTo(fromStore);
        }
        assertThat(hits(T1, query(null, Set.of(a, b, c), null, null))).containsExactly(s1, s2, s3);
    }

    @Test
    @DisplayName("the watermark tracks the last applied event; detach removes postings")
    void incremental() {
        long a = tag("a");
        ready(T1);
        long s1 = attach(ISSUE_1, a);
        attach(ISSUE_2, a);
        detach(ISSUE_2, a);
        List<TagEvent> delivered = relay();
        assertThat(index.watermark(T1)).isEqualTo(delivered.get(delivered.size() - 1).seq());
        assertThat(hits(T1, query(Set.of(a), null, null, null))).containsExactly(s1);
    }

    @Test
    @DisplayName("re-delivered (older) events are ignored")
    void redelivery() {
        long a = tag("a");
        ready(T1);
        attach(ISSUE_1, a);
        List<TagEvent> first = relay();
        detach(ISSUE_1, a);
        relay();
        index.onEvents(first);   // at-least-once: the old attach shows up again
        assertThat(hits(T1, query(Set.of(a), null, null, null))).isEmpty();
    }

    @Test
    @DisplayName("a deleted tag stops matching at once, even if late attach events arrive")
    void deletedTag() {
        long a = tag("a");
        long b = tag("b");
        ready(T1);
        long s1 = attach(ISSUE_1, a, b);
        deleteTag(a);
        relay();
        assertThat(hits(T1, query(Set.of(a), null, null, null))).isEmpty();
        assertThat(hits(T1, query(Set.of(b), null, null, null))).containsExactly(s1);
    }

    @Test
    @DisplayName("pages by entitySeq with a total")
    void paging() {
        long a = tag("a");
        ready(T1);
        List<Long> attached = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            attached.add(attach(new EntityRef("jira:issue", "p" + i), a));
        }
        relay();
        SearchPostings p1 = index.search(T1, new TagQuery(Set.of(a), null, null, null, 0, 2)).orElseThrow();
        assertThat(p1.entitySeqs()).containsExactly(attached.get(0), attached.get(1));
        assertThat(p1.total()).isEqualTo(5);
        assertThat(p1.hasMore()).isTrue();
        SearchPostings p3 = index.search(T1, new TagQuery(Set.of(a), null, null, null, attached.get(3), 2)).orElseThrow();
        assertThat(p3.entitySeqs()).containsExactly(attached.get(4));
        assertThat(p3.hasMore()).isFalse();
    }

    @Test
    @DisplayName("tenants never see each other's postings")
    void tenantIsolation() {
        long a = tag("a");
        attach(ISSUE_1, a);
        relay();
        ready(T1);
        ready(T2);
        assertThat(hits(T2, query(Set.of(a), null, null, null))).isEmpty();
        assertThat(hits(T1, query(Set.of(a), null, null, null))).hasSize(1);
    }
}
