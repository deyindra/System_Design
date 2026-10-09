package com.salesforce.einstein.hierarchy;

import com.salesforce.einstein.hierarchy.cache.RedisTreeCache;
import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.events.InProcessEventPublisher;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class RedisTreeCacheIT {
    private static final ReadToken NONE = ReadToken.NONE;

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine");

    static {
        REDIS.setExposedPorts(List.of(6379));
    }

    private static RedisClient client;
    private static StatefulRedisConnection<String, String> connection;

    private RedisTreeCache cache;
    private Fixture f;
    private Space s;

    @BeforeAll
    static void connect() {
        client = RedisClient.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        connection = client.connect();
    }

    @AfterAll
    static void disconnect() {
        connection.close();
        client.shutdown();
    }

    @BeforeEach
    void setUp() {
        cache = new RedisTreeCache(connection, Duration.ofMinutes(5), Duration.ofMinutes(5));
        f = new Fixture(10_000, cache, new InProcessEventPublisher());
        s = f.space("CACHE");
    }

    @Test
    void treeVersionOnlyRises() {
        UUID t = UUID.randomUUID();
        assertThat(cache.treeVersion(t, 1)).isEmpty();
        cache.raiseTreeVersion(t, 1, 5);
        cache.raiseTreeVersion(t, 1, 3);   // a late, older write
        assertThat(cache.treeVersion(t, 1)).hasValue(5);
        cache.raiseTreeVersion(t, 1, 6);
        assertThat(cache.treeVersion(t, 1)).hasValue(6);
        assertThat(connection.sync().ttl("tv:{" + t + "}:1")).isBetween(1L, 600L);
    }

    @Test
    void breadcrumbsComeFromTheCacheUntilAMoveRaisesTheVersion() {
        Node a = f.page(s, s.rootNodeId(), "a");
        Node b = f.page(s, s.rootNodeId(), "b");
        Node leaf = f.page(s, a.id(), "leaf");

        assertThat(f.tree.ancestors(f.alice, leaf.id(), NONE)).extracting(Crumb::id)
                .containsExactly(s.rootNodeId(), a.id());
        assertThat(cache.breadcrumb(f.tenant, leaf.id())).hasValueSatisfying(bc ->
                assertThat(bc.ids()).containsExactly(s.rootNodeId(), a.id(), leaf.id()));

        f.tree.move(f.alice, a.id(), new MoveNode(b.id(), null, null), null);

        // The cached breadcrumb is still there but stale: its tree version is behind.
        TreeCache.Breadcrumb stale = cache.breadcrumb(f.tenant, leaf.id()).orElseThrow();
        assertThat(cache.treeVersion(f.tenant, s.id()).orElseThrow()).isGreaterThan(stale.treeVersion());
        assertThat(f.tree.ancestors(f.alice, leaf.id(), NONE)).extracting(Crumb::id)
                .containsExactly(s.rootNodeId(), b.id(), a.id());
    }

    @Test
    void aRenameEvictsOneTitle() {
        Node a = f.page(s, s.rootNodeId(), "before");
        Node leaf = f.page(s, a.id(), "leaf");
        f.tree.ancestors(f.alice, leaf.id(), NONE);
        assertThat(cache.crumbs(f.tenant, List.of(a.id()))).containsKey(a.id());

        f.tree.update(f.alice, a.id(), a.version(), "after", null);

        assertThat(cache.crumbs(f.tenant, List.of(a.id()))).doesNotContainKey(a.id());
        assertThat(f.tree.ancestors(f.alice, leaf.id(), NONE)).extracting(Crumb::title).endsWith("after");
    }

    @Test
    void childrenPagesAreCachedAndEvictedByWrites() {
        Node a = f.page(s, s.rootNodeId(), "a");
        assertThat(f.tree.children(f.alice, s.rootNodeId(), 50, null, NONE).items()).hasSize(1);
        assertThat(cache.children(f.tenant, s.rootNodeId())).isPresent();

        f.page(s, s.rootNodeId(), "b");
        assertThat(cache.children(f.tenant, s.rootNodeId())).isEmpty();
        assertThat(f.tree.children(f.alice, s.rootNodeId(), 50, null, NONE).items()).extracting(Node::title)
                .containsExactly("a", "b");
        // Served from the cache now, and still ACL-filtered per reader.
        f.tree.setRestrictions(f.alice, a.id(), RestrictionOp.VIEW,
                List.of("group:eng"));
        assertThat(f.tree.children(f.bob, s.rootNodeId(), 50, null, NONE).items()).extracting(Node::title)
                .containsExactly("b");
    }
}
