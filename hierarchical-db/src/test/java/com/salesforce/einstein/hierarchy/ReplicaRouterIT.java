package com.salesforce.einstein.hierarchy;

import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.Written;
import com.salesforce.einstein.hierarchy.store.Db;
import com.salesforce.einstein.hierarchy.store.ReplicaRouter;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real LSN probes. The primary is registered as its own "replica": {@code pg_last_wal_replay_lsn()} is null there,
 * so it reports its write position and is always caught up once probed.
 */
@Testcontainers(disabledWithoutDocker = true)
class ReplicaRouterIT {
    @Test
    void tokensRouteToAReplicaOnlyOnceItHasReplayedTheWrite() {
        Fixture f = new Fixture(10_000);
        Db primary = f.db;
        Db replica = new Db("r1", Pg.dataSource());
        ReplicaRouter router = new ReplicaRouter(primary, List.of(replica), ReplicaRouter.sql(primary),
                Clock.systemUTC(), Duration.ofSeconds(1));

        Written<Space> w = f.tree.createSpace(f.alice, "LSN", "lsn");
        ReadToken token = w.token();
        assertThat(token.shard()).isEqualTo("s0");
        assertThat(token.lsn()).isPositive();

        assertThat(router.forRead(token)).isSameAs(primary);   // never probed: the replica's position is unknown
        router.refresh();
        assertThat(router.forRead(token)).isSameAs(replica);
        assertThat(router.forRead(ReadToken.NONE)).isSameAs(replica);
        assertThat(router.maxLagMs()).isLessThan(1_000);

        ReadToken ahead = new ReadToken("s0", token.lsn() + (1L << 40));
        assertThat(router.forRead(ahead)).isSameAs(primary);

        Node root = replica.read(j -> f.store.node(j, f.tenant, w.value().rootNodeId()).orElseThrow());
        assertThat(root.isRoot()).isTrue();
    }
}
