package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import com.salesforce.einstein.tagging.spi.TrendStore.TagCount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The {@link TrendStore} contract (TCK). Every adapter gets a subclass that only implements {@link #newStore()}. */
public abstract class TrendStoreContractTest {
    protected static final String T1 = "tenant-1";
    protected static final String T2 = "tenant-2";
    protected static final Instant NOW = Instant.parse("2026-01-08T12:30:00Z");
    private static final int H = TrendWindow.HOUR;
    private static final int D = TrendWindow.DAY;

    protected TrendStore store;
    private long seq;

    /** A fresh, empty store. */
    protected abstract TrendStore newStore();

    @BeforeEach
    void setUp() {
        store = newStore();
    }

    private TagEvent attached(String tenant, Instant at, Long... tagIds) {
        return new TagEvent(++seq, "s0", tenant, TagEvent.Type.TAGS_ATTACHED, 0, "jira:issue", "1", 1,
                List.of(tagIds), "alice", at);
    }

    private TagEvent deleted(String tenant, long tagId) {
        return new TagEvent(++seq, "s0", tenant, TagEvent.Type.TAG_DELETED, tagId, null, null, 0, List.of(), "alice", NOW);
    }

    /** The 24 h window ending at {@link #NOW}. */
    private List<TagCount> day(String tenant, TrendRank rank, int limit) {
        long last = TrendWindow.bucket(NOW, H);
        return store.top(tenant, H, last - 23, last, rank, limit);
    }

    @Test
    @DisplayName("attaches are counted per tag at every grain, in the bucket of the event time")
    void counts() {
        store.record("s0", List.of(attached(T1, NOW, 1L, 2L), attached(T1, NOW.minus(Duration.ofHours(3)), 1L)));
        assertThat(day(T1, TrendRank.POPULAR, 10)).containsExactly(new TagCount(1, 2, 0), new TagCount(2, 1, 0));
        long today = TrendWindow.bucket(NOW, D);
        assertThat(store.top(T1, D, today - 6, today, TrendRank.POPULAR, 10))
                .containsExactly(new TagCount(1, 2, 0), new TagCount(2, 1, 0));
    }

    @Test
    @DisplayName("a redelivered batch is not counted twice")
    void exactlyOnce() {
        List<TagEvent> batch = List.of(attached(T1, NOW, 1L), attached(T1, NOW, 1L));
        store.record("s0", batch);
        store.record("s0", batch);
        store.record("s0", List.of(batch.get(1), attached(T1, NOW, 1L)));   // overlapping redelivery
        assertThat(day(T1, TrendRank.POPULAR, 10)).containsExactly(new TagCount(1, 3, 0));
    }

    @Test
    @DisplayName("progress is per source shard: a tenant moved to a new shard restarts its seq")
    void perSourceShard() {
        TagEvent e = attached(T1, NOW, 1L);
        store.record("s0", List.of(e));
        store.record("s1", List.of(new TagEvent(1, "s1", T1, TagEvent.Type.TAGS_ATTACHED, 0, "jira:issue", "1", 1,
                List.of(1L), "alice", NOW)));
        assertThat(day(T1, TrendRank.POPULAR, 10)).containsExactly(new TagCount(1, 2, 0));
    }

    @Test
    @DisplayName("tenants are isolated")
    void tenantIsolation() {
        store.record("s0", List.of(attached(T1, NOW, 1L), attached(T2, NOW, 2L)));
        assertThat(day(T1, TrendRank.POPULAR, 10)).extracting(TagCount::tagId).containsExactly(1L);
        assertThat(day(T2, TrendRank.POPULAR, 10)).extracting(TagCount::tagId).containsExactly(2L);
    }

    @Test
    @DisplayName("popular orders by count; rising by growth over the previous window, growers only")
    void ranks() {
        List<TagEvent> events = new ArrayList<>();
        Instant yesterday = NOW.minus(Duration.ofHours(30));
        for (int i = 0; i < 5; i++) {
            events.add(attached(T1, yesterday, 1L));   // tag 1: 5 → 3, falling
        }
        for (int i = 0; i < 3; i++) {
            events.add(attached(T1, NOW, 1L, 2L));     // tag 2: 0 → 3
        }
        events.add(attached(T1, yesterday, 3L));       // tag 3: 1 → 2
        events.add(attached(T1, NOW, 3L));
        events.add(attached(T1, NOW, 3L));
        events.add(attached(T1, NOW.minus(Duration.ofHours(60)), 4L));   // tag 4: outside both windows
        store.record("s0", events);

        assertThat(day(T1, TrendRank.POPULAR, 10))
                .containsExactly(new TagCount(1, 3, 5), new TagCount(2, 3, 0), new TagCount(3, 2, 1));
        assertThat(day(T1, TrendRank.RISING, 10)).containsExactly(new TagCount(2, 3, 0), new TagCount(3, 2, 1));
        assertThat(day(T1, TrendRank.POPULAR, 1)).extracting(TagCount::tagId).containsExactly(1L);
    }

    @Test
    @DisplayName("a deleted tag's counters are dropped")
    void deleteDropsCounters() {
        store.record("s0", List.of(attached(T1, NOW, 1L, 2L)));
        store.record("s0", List.of(deleted(T1, 1L)));
        assertThat(day(T1, TrendRank.POPULAR, 10)).extracting(TagCount::tagId).containsExactly(2L);
    }

    @Test
    @DisplayName("prune removes old buckets of one grain only")
    void prune() {
        store.record("s0", List.of(attached(T1, NOW.minus(Duration.ofDays(4)), 1L), attached(T1, NOW, 2L)));
        assertThat(store.prune(H, TrendWindow.bucket(NOW.minus(Duration.ofDays(3)), H))).isEqualTo(1);
        long last = TrendWindow.bucket(NOW, H);
        assertThat(store.top(T1, H, last - 200, last, TrendRank.POPULAR, 10)).extracting(TagCount::tagId)
                .containsExactly(2L);
        long today = TrendWindow.bucket(NOW, D);
        assertThat(store.top(T1, D, today - 6, today, TrendRank.POPULAR, 10)).extracting(TagCount::tagId)
                .containsExactlyInAnyOrder(1L, 2L);
    }
}
