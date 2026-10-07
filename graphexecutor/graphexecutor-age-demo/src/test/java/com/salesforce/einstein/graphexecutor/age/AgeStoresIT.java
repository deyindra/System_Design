package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.age.demo.RandomGraph;
import com.salesforce.einstein.graphexecutor.age.demo.RecordingTopological;
import com.salesforce.einstein.graphexecutor.age.demo.RecordingTraversal;
import com.salesforce.einstein.graphexecutor.distributed.ClusterReport;
import com.salesforce.einstein.graphexecutor.distributed.Worker;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.Arrival;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import com.salesforce.einstein.graphexecutor.spi.memory.InMemoryGraphStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Each AGE/PostgreSQL store against a real database, and workers (threads of this JVM) running on them. */
@Testcontainers(disabledWithoutDocker = true)
class AgeStoresIT {
    private static final int SHARDS = 4;
    private static final NodeCodec<String> CODEC = NodeCodec.strings();

    @Container
    private static final GenericContainer<?> AGE = AgeContainers.age(Network.newNetwork());

    private static PgConnections cypher;
    private static PgConnections tables;

    @BeforeAll
    static void connect() {
        cypher = AgeContainers.connections(AGE, PgConnections.AGE);
        tables = AgeContainers.connections(AGE, List.of());
    }

    @AfterAll
    static void disconnect() {
        cypher.close();
        tables.close();
    }

    private static GraphStore<String> load(String name, Graph<String> graph) {
        AgeGraphLoader.byKey(cypher, AgeGraph.named(name), CODEC, SHARDS).create().load(graph, 50);
        return AgeGraphStore.byKey(cypher, AgeGraph.named(name), CODEC, SHARDS);
    }

    /** Every node of every page of every shard, checking that pages do not overlap. */
    private static Map<Integer, Set<String>> pages(GraphStore<String> store,
                                                   Function<Integer, Function<String, List<String>>> page) {
        Map<Integer, Set<String>> shards = new HashMap<>();
        for (int shard = 0; shard < store.shards(); shard++) {
            Set<String> seen = new HashSet<>();
            for (List<String> next = page.apply(shard).apply(null); !next.isEmpty();
                 next = page.apply(shard).apply(next.get(next.size() - 1))) {
                next.forEach(node -> assertTrue(seen.add(node), "paged twice: " + node));
            }
            shards.put(shard, seen);
        }
        return shards;
    }

    @Test
    void theGraphStoreReadsWhatTheLoaderWrote() {
        Graph<String> graph = RandomGraph.generate(4, 120, 300, false);
        graph.addNode("quote\"d key");   // an isolated node, with a key that needs escaping
        GraphStore<String> age = load("stores", graph);
        GraphStore<String> memory = InMemoryGraphStore.of(graph, SHARDS, GraphStore.byKey(CODEC::encode, SHARDS));

        assertEquals(pages(memory, shard -> after -> memory.nodes(shard, after, 7)),
                pages(age, shard -> after -> age.nodes(shard, after, 7)));
        assertEquals(pages(memory, shard -> after -> memory.sources(shard, after, 7)),
                pages(age, shard -> after -> age.sources(shard, after, 7)));
        List<String> nodes = new ArrayList<>(graph.nodes());
        Map<String, List<String>> successors = age.successors(nodes);
        Map<String, List<String>> predecessors = age.predecessors(nodes);
        for (String node : nodes) {
            assertEquals(graph.successors(node), Set.copyOf(successors.get(node)), node);
            assertEquals(graph.predecessors(node), Set.copyOf(predecessors.get(node)), node);
        }
        assertFalse(age.successors(List.of("not in the graph")).containsKey("not in the graph"));
        Graph<String> loaded = GraphStore.load(age, 13);
        assertEquals(graph.nodes(), loaded.nodes());
        assertEquals(graph.edgeCount(), loaded.edgeCount());
    }

    @Test
    void theCompletionLogKeepsTheFirstOutcome() {
        PostgresCompletionLog<String> log = new PostgresCompletionLog<>(tables, "log-run", CODEC);
        PostgresCompletionLog<String> otherRun = new PostgresCompletionLog<>(tables, "other-run", CODEC);
        log.recordAll(Map.of("a", new Outcome(Status.DONE, 0, null), "b", new Outcome(Status.FAILED, 1, "boom")));
        log.record("a", new Outcome(Status.FAILED, 5, "late"));

        assertEquals(Map.of("a", new Outcome(Status.DONE, 0, null), "b", new Outcome(Status.FAILED, 1, "boom")),
                log.outcomes(List.of("a", "b", "c")));
        assertEquals(2, log.size());
        assertTrue(otherRun.outcome("a").isEmpty(), "logs are per run");
    }

    @Test
    void theClusterStoreFencesDeadWorkersAndAdvancesOnce() throws InterruptedException {
        ClusterStore cluster = new PostgresClusterStore(tables);
        Duration ttl = Duration.ofMillis(400);
        cluster.createRun("fence", 1, ttl);
        cluster.createRun("fence", 7, ttl);   // a second worker's call changes nothing
        assertEquals(1, cluster.run("fence").orElseThrow().shards());

        assertTrue(cluster.claim("fence", 0, "old", ttl));
        assertFalse(cluster.claim("fence", 0, "new", ttl), "a live lease is not taken");
        Thread.sleep(ttl.toMillis() + 100);
        assertTrue(cluster.leases("fence").get(0).expired());
        assertTrue(cluster.claim("fence", 0, "new", Duration.ofSeconds(30)), "an expired lease is");

        assertEquals(0, cluster.beginAttempt("fence", 0, 0));
        assertEquals(1, cluster.beginAttempt("fence", 0, 0));
        cluster.send("fence", 0, 0, 0, List.of("a", "b"));
        cluster.send("fence", 0, 0, 0, List.of("c"));   // a replay's batch: dropped
        assertEquals(Set.of("a", "b"), cluster.inbox("fence", 0, 0));

        assertFalse(cluster.arrive("fence", new Arrival(0, 0, "old", 0, 1, 0, 0, 1, 2)));
        assertTrue(cluster.arrive("fence", new Arrival(0, 0, "new", 1, 1, 0, 0, 1, 2)));
        List<CompletableFuture<Void>> racing = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            racing.add(CompletableFuture.runAsync(() -> cluster.advance("fence", 0)));
        }
        racing.forEach(CompletableFuture::join);
        assertEquals(1, cluster.run("fence").orElseThrow().round(), "advanced exactly once");

        assertTrue(cluster.arrive("fence", new Arrival(1, 0, "new", 0, 0, 0, 0, 0, 0)));
        cluster.advance("fence", 1);
        ClusterReport report = ClusterReport.read(cluster, "fence");
        assertEquals(RunStatus.DONE, report.status());
        assertEquals(1, report.recoveries());
        assertEquals(Set.of("new"), report.workers());
    }

    @Test
    void threeWorkersTraverseTheStoredGraph() {
        Graph<String> graph = RandomGraph.generate(9, 150, 400, false);
        try (GraphStore<String> store = load("traversal", graph)) {
            ClusterReport report = runThree("traversal-run", worker -> Worker.traversal(
                    new RecordingTraversal.Executor(null), Set.of(AgeContainers.ROOT), store,
                    new PostgresCompletionLog<>(tables, "traversal-run", CODEC), CODEC,
                    new PostgresClusterStore(tables), config("traversal-run", worker)));

            assertEquals(RunStatus.DONE, report.status());
        }
        assertEquals(AgeContainers.depths(graph), rounds(new PostgresCompletionLog<>(tables, "traversal-run", CODEC)));
    }

    @Test
    void threeWorkersRunTheStoredDagInDependencyOrder() {
        Graph<String> dag = RandomGraph.generate(10, 120, 300, true);
        try (GraphStore<String> store = load("topological", dag)) {
            ClusterReport report = runThree("topological-run", worker -> Worker.topological(
                    new RecordingTopological.Executor(null), store,
                    new PostgresCompletionLog<>(tables, "topological-run", CODEC), CODEC,
                    new PostgresClusterStore(tables), config("topological-run", worker)));

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(dag.nodeCount(), report.done());
        }
        assertEquals(AgeContainers.levels(dag), rounds(new PostgresCompletionLog<>(tables, "topological-run", CODEC)));
    }

    @Test
    void threeWorkersTraverseAStoredUndirectedGraph() {
        Graph<String> graph = Graph.undirected();   // sparse, so there are several components
        Graph<String> arcs = RandomGraph.generate(12, 150, 160, false);
        arcs.nodes().forEach(graph::addNode);
        arcs.nodes().forEach(from -> arcs.successors(from).forEach(to -> graph.addEdge(from, to)));
        AgeGraphLoader.byKey(cypher, AgeGraph.named("undirected"), CODEC, SHARDS).create().load(graph, 50);
        try (GraphStore<String> store = AgeGraphStore.byKey(cypher, AgeGraph.named("undirected"), CODEC, SHARDS, false)) {
            assertFalse(store.isDirected());
            List<String> nodes = new ArrayList<>(graph.nodes());
            Map<String, List<String>> successors = store.successors(nodes);
            Map<String, List<String>> predecessors = store.predecessors(nodes);
            for (String node : nodes) {
                assertEquals(graph.successors(node), Set.copyOf(successors.get(node)), node);
                assertEquals(graph.successors(node), Set.copyOf(predecessors.get(node)), node);
            }
            Graph<String> loaded = GraphStore.load(store, 13);
            assertFalse(loaded.isDirected());
            assertEquals(graph.edgeCount(), loaded.edgeCount());

            ClusterReport report = runThree("undirected-run", worker -> Worker.traversal(
                    new RecordingTraversal.Executor(null), Set.of(AgeContainers.ROOT), store,
                    new PostgresCompletionLog<>(tables, "undirected-run", CODEC), CODEC,
                    new PostgresClusterStore(tables), config("undirected-run", worker)));

            assertEquals(RunStatus.DONE, report.status());
        }
        Map<String, Integer> depths = AgeContainers.depths(graph);
        assertTrue(depths.size() > 1 && depths.size() < graph.nodeCount(), "one component, not all: " + depths.size());
        assertEquals(depths, rounds(new PostgresCompletionLog<>(tables, "undirected-run", CODEC)));
    }

    private static Worker.Config config(String run, String worker) {
        return Worker.Config.of(run, worker).withMaxShards(2).withLease(Duration.ofSeconds(5), Duration.ofMillis(10))
                .withBatchSize(25);
    }

    private static ClusterReport runThree(String run, Function<String, Worker<String>> worker) {
        List<CompletableFuture<ClusterReport>> running = new ArrayList<>();
        for (int w = 0; w < 3; w++) {
            Worker<String> next = worker.apply("w" + w);
            running.add(CompletableFuture.supplyAsync(next::run));
        }
        running.forEach(CompletableFuture::join);
        return ClusterReport.read(new PostgresClusterStore(tables), run);
    }

    private static Map<String, Integer> rounds(PostgresCompletionLog<String> log) {
        Map<String, Integer> rounds = new HashMap<>();
        log.all().forEach((node, outcome) -> rounds.put(node, outcome.round()));
        return rounds;
    }
}
