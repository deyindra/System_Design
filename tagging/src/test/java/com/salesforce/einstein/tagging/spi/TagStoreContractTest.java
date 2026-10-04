package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.DuplicateTagNameException;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Page;
import com.salesforce.einstein.tagging.domain.StaleVersionException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagNames;
import com.salesforce.einstein.tagging.domain.TagQuery;
import com.salesforce.einstein.tagging.domain.Cursors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link TagStore} contract (the TCK). Every adapter, built in or third party, cloud-agnostic or
 * cloud-specific, gets a subclass that only implements {@link #newStore()}. Passing this suite is what
 * makes a backend safe to swap.
 */
public abstract class TagStoreContractTest {
    protected static final String T1 = "tenant-1";
    protected static final String T2 = "tenant-2";
    protected static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    protected static final Duration GAP = Duration.ofSeconds(10);
    private static final EntityRef ISSUE_1 = new EntityRef("jira:issue", "1");
    private static final EntityRef ISSUE_2 = new EntityRef("jira:issue", "2");
    private static final EntityRef PAGE_1 = new EntityRef("confluence:page", "1");

    private final AtomicLong ids = new AtomicLong(100);
    protected TagStore store;

    /** A fresh, empty store. */
    protected abstract TagStore newStore();

    @BeforeEach
    void setUp() {
        store = newStore();
    }

    protected Tag newTag(String tenant, String name) {
        Tag tag = Tag.create(tenant, ids.incrementAndGet(), name, "blue", "alice", NOW);
        return store.write(tenant, (UnitOfWork uow) -> uow.insertTag(tag));
    }

    protected long attach(String tenant, EntityRef e, Long... tagIds) {
        return store.write(tenant, (UnitOfWork uow) -> {
            long seq = uow.entitySeq(e, true);
            uow.attach(e, seq, List.of(tagIds), "alice", NOW);
            return seq;
        });
    }

    private <T> T read(String tenant, java.util.function.Function<TagReader, T> f) {
        return store.read(tenant, ReadPreference.strong(), f);
    }

    private List<TagEvent> drain() {
        List<TagEvent> out = new ArrayList<>();
        int relayed;
        do {
            relayed = store.relay(100, GAP, out::addAll);
        } while (relayed > 0);   // keep relaying until empty
        return out;
    }

    // --- tags ------------------------------------------------------------------------------------

    @Test
    @DisplayName("tags are found by id and by normalized name")
    void findTag() {
        Tag t = newTag(T1, "Bug Bash");
        assertThat(read(T1, (TagReader r) -> r.findTag(t.tagId()))).contains(t);
        assertThat(read(T1, (TagReader r) -> r.findTagByName(TagNames.normalize("  BUG   bash ")))).contains(t);
        assertThat(read(T1, (TagReader r) -> r.findTags(List.of(t.tagId(), 999L)))).containsExactly(t);
        assertThat(read(T1, TagReader::countLiveTags)).isEqualTo(1);
    }

    @Test
    @DisplayName("names are unique per tenant, not globally")
    void uniqueNamePerTenant() {
        newTag(T1, "release");
        assertThatThrownBy(() -> newTag(T1, "RELEASE")).isInstanceOf(DuplicateTagNameException.class);
        assertThat(newTag(T2, "release").tenantId()).isEqualTo(T2);
    }

    @Test
    @DisplayName("updateTag is a compare-and-set on version")
    void updateCas() {
        Tag t = newTag(T1, "alpha");
        Tag renamed = store.write(T1, (UnitOfWork uow) -> uow.updateTag(t.renamed("beta", NOW), t.version()));
        assertThat(renamed.version()).isEqualTo(t.version() + 1);
        assertThat(read(T1, (TagReader r) -> r.findTagByName("beta"))).isPresent();
        assertThat(read(T1, (TagReader r) -> r.findTagByName("alpha"))).isEmpty();
        assertThatThrownBy(() -> store.write(T1, (UnitOfWork uow) -> uow.updateTag(t.renamed("gamma", NOW), t.version())))
                .isInstanceOf(StaleVersionException.class);
    }

    @Test
    @DisplayName("renaming onto a taken name fails; a tombstoned tag frees its name")
    void renameCollisionAndTombstone() {
        Tag a = newTag(T1, "a");
        Tag b = newTag(T1, "b");
        assertThatThrownBy(() -> store.write(T1, (UnitOfWork uow) -> uow.updateTag(b.renamed("A", NOW), b.version())))
                .isInstanceOf(DuplicateTagNameException.class);
        store.write(T1, (UnitOfWork uow) -> uow.updateTag(a.tombstoned(NOW), a.version()));
        assertThat(read(T1, (TagReader r) -> r.findTag(a.tagId()))).get().extracting(Tag::deleted).isEqualTo(true);
        assertThat(read(T1, TagReader::countLiveTags)).isEqualTo(1);
        assertThat(newTag(T1, "a").name()).isEqualTo("a");
    }

    @Test
    @DisplayName("listTags filters by prefix and pages by name")
    void listTags() {
        for (String n : List.of("team-a", "team-b", "team-c", "other", "team_x")) {
            newTag(T1, n);
        }
        Page<Tag> p1 = read(T1, (TagReader r) -> r.listTags("team-", null, 2));
        assertThat(p1.items()).extracting(Tag::name).containsExactly("team-a", "team-b");
        Page<Tag> p2 = read(T1, (TagReader r) -> r.listTags("team-", Cursors.name(p1.nextCursor()), 2));
        assertThat(p2.items()).extracting(Tag::name).containsExactly("team-c");
        assertThat(p2.nextCursor()).isNull();
        assertThat(read(T1, (TagReader r) -> r.listTags("team_", null, 10)).items()).extracting(Tag::name)
                .containsExactly("team_x");   // '_' is literal, not a wildcard
    }

    // --- assignments -----------------------------------------------------------------------------

    @Test
    @DisplayName("attach and detach are idempotent and report only real changes")
    void attachDetachIdempotent() {
        Tag a = newTag(T1, "a");
        Tag b = newTag(T1, "b");
        Set<Long> first = store.write(T1, (UnitOfWork uow) -> uow.attach(ISSUE_1, uow.entitySeq(ISSUE_1, true),
                List.of(a.tagId(), b.tagId()), "u", NOW));
        Set<Long> again = store.write(T1, (UnitOfWork uow) -> uow.attach(ISSUE_1, uow.entitySeq(ISSUE_1, true),
                List.of(a.tagId()), "u", NOW));
        assertThat(first).containsExactlyInAnyOrder(a.tagId(), b.tagId());
        assertThat(again).isEmpty();
        assertThat(read(T1, (TagReader r) -> r.tagsOf(ISSUE_1))).containsExactly(a.tagId(), b.tagId());

        Set<Long> removed = store.write(T1, (UnitOfWork uow) -> uow.detach(ISSUE_1, List.of(a.tagId(), 12345L)));
        assertThat(removed).containsExactly(a.tagId());
        assertThat(read(T1, (TagReader r) -> r.tagsOf(ISSUE_1))).containsExactly(b.tagId());
    }

    @Test
    @DisplayName("entitySeq is stable per entity and absent until created")
    void entitySeqStable() {
        assertThat(store.write(T1, (UnitOfWork uow) -> uow.entitySeq(ISSUE_1, false))).isZero();
        long s1 = store.write(T1, (UnitOfWork uow) -> uow.entitySeq(ISSUE_1, true));
        long s2 = store.write(T1, (UnitOfWork uow) -> uow.entitySeq(ISSUE_1, true));
        assertThat(s1).isPositive().isEqualTo(s2);
        assertThat(read(T1, (TagReader r) -> r.resolveEntities(List.of(s1, 99999L)))).isEqualTo(Map.of(s1, ISSUE_1));
    }

    @Test
    @DisplayName("entitiesOf pages by entitySeq and filters by type")
    void entitiesOf() {
        Tag t = newTag(T1, "t");
        long s1 = attach(T1, ISSUE_1, t.tagId());
        long s2 = attach(T1, ISSUE_2, t.tagId());
        long s3 = attach(T1, PAGE_1, t.tagId());
        List<EntityHit> page = read(T1, (TagReader r) -> r.entitiesOf(t.tagId(), null, 0, 2));
        assertThat(page).extracting(EntityHit::entitySeq).containsExactly(s1, s2, s3);   // limit + 1 rows
        assertThat(read(T1, (TagReader r) -> r.entitiesOf(t.tagId(), null, s2, 2))).extracting(EntityHit::entity)
                .containsExactly(PAGE_1);
        assertThat(read(T1, (TagReader r) -> r.entitiesOf(t.tagId(), "confluence:page", 0, 10))).extracting(EntityHit::entity)
                .containsExactly(PAGE_1);
    }

    @Test
    @DisplayName("search applies all / any / none, the type filter and the cursor")
    void search() {
        Tag a = newTag(T1, "a");
        Tag b = newTag(T1, "b");
        Tag c = newTag(T1, "c");
        long s1 = attach(T1, ISSUE_1, a.tagId(), b.tagId());
        long s2 = attach(T1, ISSUE_2, a.tagId(), c.tagId());
        long s3 = attach(T1, PAGE_1, b.tagId());

        assertThat(hits(new TagQuery(Set.of(a.tagId()), null, null, null, 0, 10))).containsExactly(s1, s2);
        assertThat(hits(new TagQuery(Set.of(a.tagId(), b.tagId()), null, null, null, 0, 10))).containsExactly(s1);
        assertThat(hits(new TagQuery(null, Set.of(b.tagId(), c.tagId()), null, null, 0, 10))).containsExactly(s1, s2, s3);
        assertThat(hits(new TagQuery(Set.of(a.tagId()), null, Set.of(c.tagId()), null, 0, 10))).containsExactly(s1);
        assertThat(hits(new TagQuery(Set.of(a.tagId()), Set.of(c.tagId()), null, null, 0, 10))).containsExactly(s2);
        assertThat(hits(new TagQuery(null, Set.of(b.tagId()), null, "confluence:page", 0, 10))).containsExactly(s3);
        assertThat(hits(new TagQuery(null, Set.of(b.tagId(), c.tagId()), null, null, s1, 1))).containsExactly(s2, s3);
    }

    private List<Long> hits(TagQuery q) {
        return read(T1, (TagReader r) -> r.search(q)).stream().map(EntityHit::entitySeq).toList();
    }

    // --- transactions and outbox -----------------------------------------------------------------

    @Test
    @DisplayName("a failed write rolls back rows and its outbox event")
    void rollback() {
        Tag t = newTag(T1, "t");
        drain();
        assertThatThrownBy(() -> store.write(T1, (UnitOfWork uow) -> {
            long seq = uow.entitySeq(ISSUE_1, true);
            uow.attach(ISSUE_1, seq, List.of(t.tagId()), "u", NOW);
            uow.adjustUsage(Map.of(t.tagId(), 1L));
            uow.append(TagEvent.assignment(TagEvent.Type.TAGS_ATTACHED, T1, ISSUE_1, seq, List.of(t.tagId()), "alice", NOW));
            throw new IllegalStateException("boom");
        })).hasMessage("boom");
        assertThat(read(T1, (TagReader r) -> r.tagsOf(ISSUE_1))).isEmpty();
        assertThat(read(T1, (TagReader r) -> r.usage(List.of(t.tagId())))).doesNotContainEntry(t.tagId(), 1L);
        assertThat(drain()).isEmpty();
    }

    @Test
    @DisplayName("relay publishes in seq order, stamps shard + seq, and advances the watermark")
    void relayOrder() {
        long s1 = store.write(T1, (UnitOfWork uow) -> uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T1, 1, "alice", NOW)));
        long s2 = store.write(T2, (UnitOfWork uow) -> uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T2, 2, "alice", NOW)));
        long s3 = store.write(T1, (UnitOfWork uow) -> uow.append(TagEvent.tag(TagEvent.Type.TAG_DELETED, T1, 1, "alice", NOW)));
        List<TagEvent> events = drain();
        assertThat(events).extracting(TagEvent::seq).containsExactly(s1, s2, s3);
        assertThat(events).extracting(TagEvent::shard).containsOnly(store.shardId());
        assertThat(events.get(2).type()).isEqualTo(TagEvent.Type.TAG_DELETED);
        assertThat(read(T1, TagReader::relayedSeq)).isEqualTo(s3);
        assertThat(drain()).isEmpty();
    }

    @Test
    @DisplayName("latestSeq is the tenant's highest committed outbox seq, relayed or not")
    void latestSeq() {
        assertThat(read(T1, TagReader::latestSeq)).isZero();
        long s1 = store.write(T1, (UnitOfWork uow) -> uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T1, 1, "alice", NOW)));
        store.write(T2, (UnitOfWork uow) -> uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T2, 2, "alice", NOW)));
        assertThat(read(T1, TagReader::latestSeq)).isEqualTo(s1);
        drain();
        assertThat(read(T1, TagReader::latestSeq)).isEqualTo(s1);
    }

    @Test
    @DisplayName("relay is at-least-once: a failing sink doesn't advance the position")
    void relayAtLeastOnce() {
        long s1 = store.write(T1, (UnitOfWork uow) -> uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, T1, 1, "alice", NOW)));
        assertThatThrownBy(() -> store.relay(10, GAP, batch -> {
            throw new IllegalStateException("broker down");
        })).hasMessage("broker down");
        assertThat(read(T1, TagReader::relayedSeq)).isLessThan(s1);
        assertThat(store.oldestUnrelayed()).isPresent();   // the lag gauge sees the stuck event
        assertThat(drain()).extracting(TagEvent::seq).containsExactly(s1);
        assertThat(store.oldestUnrelayed()).isEmpty();
    }

    @Test
    @DisplayName("event payloads round-trip through the outbox")
    void eventRoundTrip() {
        long seq = attach(T1, ISSUE_1);
        TagEvent e = TagEvent.assignment(TagEvent.Type.TAGS_DETACHED, T1, ISSUE_1, seq, List.of(7L, 8L), "alice", NOW);
        store.write(T1, (UnitOfWork uow) -> uow.append(e));
        TagEvent got = drain().get(0);
        assertThat(got.entity()).isEqualTo(ISSUE_1);
        assertThat(got.tagIds()).containsExactly(7L, 8L);
        assertThat(got.entitySeq()).isEqualTo(seq);
        assertThat(got.actor()).isEqualTo("alice");
        assertThat(got.at()).isEqualTo(NOW);
    }

    // --- counters, purge, idempotency, isolation ---------------------------------------------------

    @Test
    @DisplayName("usage counters sum their deltas; purge removes assignments and counters")
    void usageAndPurge() {
        Tag t = newTag(T1, "t");
        attach(T1, ISSUE_1, t.tagId());
        attach(T1, ISSUE_2, t.tagId());
        attach(T1, PAGE_1, t.tagId());
        for (int i = 0; i < 3; i++) {
            store.write(T1, (UnitOfWork uow) -> {
                uow.adjustUsage(Map.of(t.tagId(), 1L));
                return null;
            });
        }
        assertThat(read(T1, (TagReader r) -> r.usage(List.of(t.tagId())))).containsEntry(t.tagId(), 3L);

        assertThat(store.write(T1, (UnitOfWork uow) -> uow.purgeAssignments(t.tagId(), 2))).isEqualTo(2);
        assertThat(store.write(T1, (UnitOfWork uow) -> uow.purgeAssignments(t.tagId(), 2))).isEqualTo(1);
        assertThat(read(T1, (TagReader r) -> r.entitiesOf(t.tagId(), null, 0, 10))).isEmpty();
        assertThat(read(T1, (TagReader r) -> r.usage(List.of(t.tagId())))).isEmpty();
    }

    @Test
    @DisplayName("idempotency records are per tenant and unique per key")
    void idempotency() {
        IdempotencyRecord r = new IdempotencyRecord(T1, "k1", "h", "{}", NOW);
        store.write(T1, (UnitOfWork uow) -> {
            uow.saveIdempotency(r);
            return null;
        });
        assertThat(store.write(T1, (UnitOfWork uow) -> uow.findIdempotency("k1"))).contains(r);
        assertThat(store.write(T2, (UnitOfWork uow) -> uow.findIdempotency("k1"))).isEmpty();
        assertThatThrownBy(() -> store.write(T1, (UnitOfWork uow) -> {
            uow.saveIdempotency(r);
            return null;
        })).isInstanceOf(DuplicateKeyException.class);
        assertThat(store.pruneIdempotency(NOW.plusSeconds(1))).isEqualTo(1);
    }

    @Test
    @DisplayName("a claimed idempotency key gets its response in the same unit of work")
    void idempotencyClaimThenComplete() {
        store.write(T1, (UnitOfWork uow) -> {
            uow.saveIdempotency(new IdempotencyRecord(T1, "k2", "h", "", NOW));
            uow.completeIdempotency("k2", "{\"done\":true}");
            return null;
        });
        assertThat(store.write(T1, (UnitOfWork uow) -> uow.findIdempotency("k2")))
                .hasValueSatisfying(x -> assertThat(x.response()).isEqualTo("{\"done\":true}"));
        assertThatThrownBy(() -> store.write(T1, (UnitOfWork uow) -> {
            uow.completeIdempotency("never-claimed", "{}");
            return null;
        })).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a tenant never sees another tenant's tags, entities or assignments")
    void tenantIsolation() {
        Tag t = newTag(T1, "secret");
        long seq = attach(T1, ISSUE_1, t.tagId());
        assertThat(read(T2, (TagReader r) -> r.findTag(t.tagId()))).isEmpty();
        assertThat(read(T2, (TagReader r) -> r.tagsOf(ISSUE_1))).isEmpty();
        assertThat(read(T2, (TagReader r) -> r.entitiesOf(t.tagId(), null, 0, 10))).isEmpty();
        assertThat(read(T2, (TagReader r) -> r.resolveEntities(List.of(seq)))).isEmpty();
        assertThat(read(T2, (TagReader r) -> r.search(new TagQuery(Set.of(t.tagId()), null, null, null, 0, 10)))).isEmpty();
        assertThatThrownBy(() -> store.write(T2, (UnitOfWork uow) -> uow.insertTag(Tag.create(T1, 1, "x", null, "u", NOW))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("scanAssignments streams live-tag postings only")
    void scan() {
        Tag live = newTag(T1, "live");
        Tag gone = newTag(T1, "gone");
        long s1 = attach(T1, ISSUE_1, live.tagId(), gone.tagId());
        store.write(T1, (UnitOfWork uow) -> uow.updateTag(gone.tombstoned(NOW), gone.version()));
        List<Posting> postings = new ArrayList<>();
        store.read(T1, ReadPreference.snapshotRead(), r -> {
            r.scanAssignments(postings::add);
            return null;
        });
        assertThat(postings).containsExactly(new Posting(live.tagId(), "jira:issue", s1));
    }
}
