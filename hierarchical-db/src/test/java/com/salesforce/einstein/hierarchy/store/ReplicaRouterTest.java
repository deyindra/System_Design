package com.salesforce.einstein.hierarchy.store;

import com.salesforce.einstein.hierarchy.domain.ReadToken;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicaRouterTest {
    /** Never connects: the router only needs Db identities here. */
    private static Db db(String name) {
        return new Db(name, new DriverManagerDataSource("jdbc:postgresql://unused/" + name));
    }

    private static final class MutableClock extends Clock {
        long ms = 1_000_000;

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
            return Instant.ofEpochMilli(ms);
        }
    }

    private static final class FakeLsns implements ReplicaRouter.LsnSource {
        long primary;
        final Map<Db, Long> replay = new HashMap<>();
        final Set<Db> down = new HashSet<>();

        @Override
        public long primaryLsn() {
            return primary;
        }

        @Override
        public long replayLsn(Db replica) {
            if (down.contains(replica)) {
                throw new IllegalStateException("connection refused");
            }
            return replay.getOrDefault(replica, 0L);
        }
    }

    private final Db primary = db("p");
    private final Db r1 = db("r1");
    private final Db r2 = db("r2");
    private final MutableClock clock = new MutableClock();
    private final FakeLsns lsns = new FakeLsns();
    private final ReplicaRouter router = new ReplicaRouter(primary, List.of(r1, r2), lsns, clock,
            Duration.ofMillis(500));

    @Test
    void noReplicasMeansPrimary() {
        ReplicaRouter none = new ReplicaRouter(primary, List.of(), lsns, clock, Duration.ofSeconds(1));
        none.refresh();
        assertThat(none.forRead(ReadToken.NONE)).isSameAs(primary);
        assertThat(none.maxLagMs()).isZero();
    }

    @Test
    void unprobedReplicasAreNotUsed() {
        assertThat(router.forRead(ReadToken.NONE)).isSameAs(primary);
    }

    @Test
    void tokenRoutesOnlyToReplicasThatReplayedIt() {
        lsns.primary = 100;
        lsns.replay.put(r1, 100L);
        lsns.replay.put(r2, 50L);
        router.refresh();
        ReadToken t = new ReadToken("s0", 100);
        for (int i = 0; i < 10; i++) {
            assertThat(router.forRead(t)).isSameAs(r1);
        }
        assertThat(router.forRead(new ReadToken("s0", 101))).isSameAs(primary);
    }

    @Test
    void lagIsTheAgeOfTheNewestPrimarySampleReplayed() {
        lsns.primary = 100;
        lsns.replay.put(r1, 100L);
        lsns.replay.put(r2, 100L);
        router.refresh();                     // t0: primary at 100, both caught up
        clock.ms += 400;
        lsns.primary = 200;
        lsns.replay.put(r1, 200L);            // r1 keeps up, r2 stays at 100
        router.refresh();
        assertThat(router.maxLagMs()).isEqualTo(400);
        clock.ms += 200;
        lsns.primary = 300;
        lsns.replay.put(r1, 300L);
        router.refresh();                     // r2's newest replayed sample is now 600 ms old: beyond max-lag
        assertThat(router.maxLagMs()).isEqualTo(600);
        for (int i = 0; i < 10; i++) {
            assertThat(router.forRead(ReadToken.NONE)).isSameAs(r1);
        }
    }

    @Test
    void unreachableReplicaIsSkippedUntilItAnswers() {
        lsns.primary = 10;
        lsns.replay.put(r1, 10L);
        lsns.replay.put(r2, 10L);
        lsns.down.add(r1);
        router.refresh();
        for (int i = 0; i < 10; i++) {
            assertThat(router.forRead(ReadToken.NONE)).isSameAs(r2);
        }
        lsns.down.add(r2);
        router.refresh();
        assertThat(router.forRead(ReadToken.NONE)).isSameAs(primary);
        lsns.down.clear();
        router.refresh();
        assertThat(router.forRead(ReadToken.NONE)).isIn(r1, r2);
    }
}
