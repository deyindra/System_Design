package com.salesforce.einstein.tagging.index;

import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.TagSearchIndex;
import com.salesforce.einstein.tagging.spi.TagSearchIndexContractTest;
import com.salesforce.einstein.tagging.spi.TagStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class RoaringInvertedIndexTest extends TagSearchIndexContractTest {
    /** Runs bootstraps only when the test says so, to observe the LOADING state. */
    private final Queue<Runnable> pending = new ArrayDeque<>();
    private boolean manual;

    @Override
    protected TagSearchIndex newIndex(Function<String, TagStore> storeOf) {
        Executor executor = r -> {
            if (manual) {
                pending.add(r);
            } else {
                r.run();
            }
        };
        return new RoaringInvertedIndex(storeOf, 100, executor);
    }

    @Test
    @DisplayName("events delivered while a tenant is loading are buffered, then replayed")
    void bufferWhileLoading() {
        long a = tag("a");
        manual = true;
        assertThat(index.search(T1, query(Set.of(a), null, null, null))).isEmpty();   // LOADING
        long s1 = attach(ISSUE_1, a);
        relay();                                       // buffered
        long s2 = attach(ISSUE_2, a);              // committed but not yet relayed
        pending.remove().run();                        // snapshot sees s1 and s2; relayed position covers s1
        relay();                                       // s2's event applies on top: no double counting
        assertThat(hits(T1, query(Set.of(a), null, null, null))).containsExactly(s1, s2);
    }

    @Test
    @DisplayName("a snapshot older than an already-delivered event is refused (retry, then give up)")
    void refusesStaleSnapshot() {
        long a = tag("a");
        relay();
        // An event the store's relay position doesn't cover yet was delivered (its relay txn is in flight).
        index.onEvents(List.of(TagEvent.assignment(TagEvent.Type.TAGS_ATTACHED, T1,
                new EntityRef("jira:issue", "x"), 1, List.of(a), "alice", NOW).sequenced("s0", 1_000)));
        assertThat(index.search(T1, query(Set.of(a), null, null, null))).isEmpty();
        assertThat(index.watermark(T1)).isEqualTo(-1);
        assertThat(((RoaringInvertedIndex) index).loadedTenants()).isZero();   // dropped, retried on a later search
    }

    @Test
    @DisplayName("an event from another shard drops the tenant (it moved), forcing a rebuild")
    void shardMove() {
        long a = tag("a");
        ready(T1);
        index.onEvents(List.of(TagEvent.tag(TagEvent.Type.TAG_CREATED, T1, a, "alice", NOW).sequenced("s9", 1)));
        assertThat(index.watermark(T1)).isEqualTo(-1);
    }
}
