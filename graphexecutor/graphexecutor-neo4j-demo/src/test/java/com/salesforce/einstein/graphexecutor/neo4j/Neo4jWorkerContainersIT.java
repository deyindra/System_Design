package com.salesforce.einstein.graphexecutor.neo4j;

import com.github.dockerjava.api.DockerClient;
import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.distributed.ClusterReport;
import com.salesforce.einstein.graphexecutor.neo4j.demo.RandomGraph;
import com.salesforce.einstein.graphexecutor.neo4j.demo.RecordingTopological;
import com.salesforce.einstein.graphexecutor.neo4j.demo.RecordingTraversal;
import com.salesforce.einstein.graphexecutor.neo4j.demo.TaskRuns;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cluster as deployed: Neo4j in one container, three workers in three more (the module's image), one of
 * which is killed ({@code docker kill}: SIGKILL, no shutdown hook, leases not released) while it is running a
 * task. The others take its shards over when its leases expire, and the run finishes with the result a single
 * process computes, without running again any task the dead worker had logged.
 */
@Testcontainers(disabledWithoutDocker = true)
class Neo4jWorkerContainersIT {
    private static final int SHARDS = 6;
    private static final String VICTIM = "w0";
    private static final NodeCodec<String> CODEC = NodeCodec.strings();
    private static final Network NETWORK = Network.newNetwork();
    private static final ImageFromDockerfile IMAGE = Neo4jContainers.workerImage();

    @Container
    private static final GenericContainer<?> NEO4J = Neo4jContainers.neo4j(NETWORK);

    private static Driver driver;
    private static Neo4jSessions sessions;

    @BeforeAll
    static void connect() {
        driver = Neo4jContainers.driver(NEO4J);
        sessions = new Neo4jSessions(driver, "neo4j");
    }

    @AfterAll
    static void disconnect() {
        driver.close();
    }

    @Test
    void aTraversalSurvivesAWorkerKilledMidTask() {
        Graph<String> graph = RandomGraph.generate(21, 240, 720, false);
        Map<String, Integer> expected = Neo4jContainers.depths(graph);

        Map<String, Outcome> outcomes = runAndKill("Traversal", graph, RecordingTraversal.class,
                Map.of("ROOTS", Neo4jContainers.ROOT));

        Map<String, Integer> rounds = new HashMap<>();
        outcomes.forEach((node, outcome) -> rounds.put(node, outcome.round()));
        assertEquals(expected, rounds, "each node at its breadth-first depth");
    }

    @Test
    void aDependencyOrderedRunSurvivesAWorkerKilledMidTask() {
        Graph<String> dag = RandomGraph.generate(22, 200, 500, true);
        Map<String, Integer> expected = Neo4jContainers.levels(dag);

        Map<String, Outcome> outcomes = runAndKill("Topological", dag, RecordingTopological.class, Map.of());

        Map<String, Integer> rounds = new HashMap<>();
        outcomes.forEach((node, outcome) -> rounds.put(node, outcome.round()));
        assertEquals(expected, rounds, "each node in the round after its last dependency");
    }

    /**
     * Loads {@code graph}, starts three workers, kills {@link #VICTIM} once it is inside a task, waits for the
     * run and checks what every run must satisfy; returns the logged outcomes for the caller's checks.
     */
    private Map<String, Outcome> runAndKill(String name, Graph<String> graph, Class<?> factory,
                                            Map<String, String> env) {
        Neo4jGraphLoader.byKey(sessions, Neo4jGraph.named(name), CODEC, SHARDS).create().load(graph, 200);
        String run = name.toLowerCase() + "-run";
        List<GenericContainer<?>> workers = new ArrayList<>();
        for (int w = 0; w < 3; w++) {
            String id = "w" + w;
            GenericContainer<?> worker = new GenericContainer<>(IMAGE);
            worker.setNetwork(NETWORK);
            worker.addEnv("NEO4J_URI", "bolt://" + Neo4jContainers.ALIAS + ":7687");
            worker.addEnv("NEO4J_PASSWORD", Neo4jContainers.PASSWORD);
            worker.addEnv("GRAPH", name);
            worker.addEnv("SHARDS", String.valueOf(SHARDS));
            worker.addEnv("RUN_ID", run);
            worker.addEnv("WORKER_ID", id);
            worker.addEnv("WORKER_FACTORY", factory.getName());
            worker.addEnv("SHARDS_PER_WORKER", "2");
            worker.addEnv("LEASE_TTL", "PT3S");
            worker.addEnv("POLL_INTERVAL", "PT0.05S");
            worker.addEnv("BATCH_SIZE", "50");
            worker.addEnv("TASK_TIME", "PT0.05S");
            env.forEach(worker::addEnv);
            worker.setLogConsumers(List.of(frame -> System.out.print("[" + id + "] " + frame.getUtf8String())));
            workers.add(worker);
        }
        try {
            Startables.deepStart(workers).join();
            Neo4jClusterStore cluster = new Neo4jClusterStore(sessions);
            Neo4jCompletionLog<String> log = new Neo4jCompletionLog<>(sessions, run, CODEC);

            waitFor(Duration.ofMinutes(2), "the victim to start a task", () -> !TaskRuns.ranBy(sessions, run, VICTIM).isEmpty());
            kill(DockerClientFactory.instance().client(), workers.get(0));
            Set<String> loggedBeforeKill = log.all().keySet();

            waitFor(Duration.ofMinutes(3), "the run to finish",
                    () -> cluster.run(run).map(state -> state.status() != RunStatus.RUNNING).orElse(false));
            ClusterReport report = ClusterReport.read(cluster, run);
            Map<String, Integer> runs = TaskRuns.counts(sessions, run);
            Set<String> ranByVictim = TaskRuns.ranBy(sessions, run, VICTIM);
            Map<String, Outcome> outcomes = log.all();

            assertEquals(RunStatus.DONE, report.status(), report.toString());
            assertTrue(report.recoveries() >= 1, "the victim's shard round was replayed: " + report);
            assertTrue(report.workers().containsAll(Set.of("w1", "w2")), report.toString());
            assertTrue(outcomes.values().stream().allMatch(outcome -> outcome.status() == Status.DONE));
            for (String node : loggedBeforeKill) {
                assertEquals(1, runs.get(node), node + " was logged before the kill, and must not run again");
            }
            runs.forEach((node, count) -> assertTrue(count == 1 || ranByVictim.contains(node),
                    node + " ran " + count + " times, and the victim never ran it"));
            assertEquals(outcomes.keySet(), runs.keySet(), "every task that ran was logged");
            return outcomes;
        } finally {
            workers.forEach(GenericContainer::stop);
        }
    }

    /** {@code docker kill}. The client is Testcontainers' shared one, which must not be closed. */
    private static void kill(DockerClient docker, GenericContainer<?> container) {
        docker.killContainerCmd(container.getContainerId()).exec();
    }

    private static void waitFor(Duration timeout, String what, Callable<Boolean> condition) {
        await(what).atMost(timeout).pollInterval(Duration.ofMillis(20)).until(condition);
    }
}
