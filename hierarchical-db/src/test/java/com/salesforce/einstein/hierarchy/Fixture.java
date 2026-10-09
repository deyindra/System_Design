package com.salesforce.einstein.hierarchy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.salesforce.einstein.hierarchy.cache.NoopTreeCache;
import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.domain.CreateNode;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.events.CacheInvalidator;
import com.salesforce.einstein.hierarchy.events.InProcessEventPublisher;
import com.salesforce.einstein.hierarchy.events.OutboxRelay;
import com.salesforce.einstein.hierarchy.service.MoveJobWorker;
import com.salesforce.einstein.hierarchy.service.TreeService;
import com.salesforce.einstein.hierarchy.spi.EventPublisher;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import com.salesforce.einstein.hierarchy.spi.TreeEventListener;
import com.salesforce.einstein.hierarchy.store.Db;
import com.salesforce.einstein.hierarchy.store.ReplicaRouter;
import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.store.TreeStore;
import com.salesforce.einstein.hierarchy.store.TreeVerifier;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import com.salesforce.einstein.hierarchy.tenant.SnowflakeIdGenerator;
import com.salesforce.einstein.hierarchy.tenant.TenantDirectory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** The service wired by hand over the shared container, with a fresh tenant. The worker only runs when told to. */
public final class Fixture {
    private static final AtomicInteger WORKERS = new AtomicInteger();
    public static final ObjectMapper JSON = JsonMapper.builder().findAndAddModules().build();

    public final UUID tenant = UUID.randomUUID();
    public final Caller alice = new Caller(tenant, "alice", Set.of("eng"));
    public final Caller bob = new Caller(tenant, "bob", Set.of("sales"));
    public final Db db;
    public final Shard shard;
    public final ShardRouter router;
    public final TreeStore store = new TreeStore(JSON);
    public final TreeService tree;
    public final MoveJobWorker worker;
    public final OutboxRelay relay;
    /** Every event the relay delivered, in order. */
    public final List<TreeEvent> events = new CopyOnWriteArrayList<>();

    public Fixture(int syncMoveLimit) {
        this(syncMoveLimit, new NoopTreeCache(), new InProcessEventPublisher());
    }

    public Fixture(int syncMoveLimit, TreeCache cache, EventPublisher publisher) {
        db = new Db("s0", Pg.dataSource());
        shard = new Shard("s0", db, new ReplicaRouter(db, List.of(), ReplicaRouter.sql(db), Clock.systemUTC(),
                Duration.ofSeconds(1)));
        router = new ShardRouter(new TenantDirectory(List.of("s0"), Map.of()), List.of(shard));
        int workerId = WORKERS.incrementAndGet() % 1024;
        tree = new TreeService(router, store, new SnowflakeIdGenerator(workerId, Clock.systemUTC()), cache,
                new TreeService.Settings(syncMoveLimit, Duration.ofSeconds(2), 3));
        worker = new MoveJobWorker(router, store, cache, new MoveJobWorker.Settings(2, Duration.ofSeconds(30), 1_000,
                Duration.ofMillis(10), false, Duration.ofSeconds(2)), "it-" + workerId);
        TreeEventListener recorder = events::addAll;
        relay = new OutboxRelay(router, store, publisher, List.of(new CacheInvalidator(cache), recorder),
                OutboxRelay.Settings.defaults(), new SimpleMeterRegistry());
    }

    public Space space(String key) {
        return tree.createSpace(alice, key, key + " space").value();
    }

    public Node page(Space s, long parentId, String title) {
        return create(s, parentId, title, "page");
    }

    public Node create(Space s, long parentId, String title, String type) {
        return tree.create(alice, s.id(), new CreateNode(parentId, title, type, null, null, null), null).value();
    }

    public List<String> violations(Space s) {
        return db.read(j -> TreeVerifier.violations(j, tenant, s.id()));
    }

    /** The node as stored, without any overlay. */
    public Node stored(long id) {
        return db.read(j -> store.node(j, tenant, id).orElseThrow());
    }
}
