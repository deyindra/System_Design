package com.salesforce.einstein.graphexecutor.age;

import com.github.dockerjava.api.DockerClient;
import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.age.demo.RandomGraph;
import com.salesforce.einstein.graphexecutor.age.demo.RecordingTopological;
import com.salesforce.einstein.graphexecutor.age.demo.RecordingTraversal;
import com.salesforce.einstein.graphexecutor.age.demo.TaskRuns;
import com.salesforce.einstein.graphexecutor.distributed.ClusterReport;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
 * The cluster as deployed: AGE in one container, three workers in three more (the module's image), one of
 * which is killed ({@code docker kill}: SIGKILL, no shutdown hook, leases not released) while it is running a
 * task. The others take its shards over when its leases expire, and the run finishes with the result a single
 * process computes, without running again any task the dead worker had logged.
 */
@Testcontainers(disabledWithoutDocker = true)
class AgeWorkerContainersIT {
    private static final int SHARDS = 6;
    private static final String VICTIM = "w0";
    private static final NodeCodec<String> CODEC = NodeCodec.strings();
    private static final Network NETWORK = Network.newNetwork();
    private static final ImageFromDockerfile IMAGE = AgeContainers.workerImage();

    @Container
    private static final GenericContainer<?> AGE = AgeContainers.age(NETWORK);

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

    @Test
    void aTraversalSurvivesAWorkerKilledMidTask() {
        Graph<String> graph = RandomGraph.generate(21, 240, 720, false);
        Map<String, Integer> expected = AgeContainers.depths(graph);

        Map<String, Outcome> outcomes = runAndKill("traversal", graph, RecordingTraversal.class,
                Map.of("ROOTS", AgeContainers.ROOT));

        Map<String, Integer> rounds = new HashMap<>();
        outcomes.forEach((node, outcome) -> rounds.put(node, outcome.round()));
        assertEquals(expected, rounds, "each node at its breadth-first depth");
    }

    @Test
    void aDependencyOrderedRunSurvivesAWorkerKilledMidTask() {
        Graph<String> dag = RandomGraph.generate(22, 200, 500, true);
        Map<String, Integer> expected = AgeContainers.levels(dag);

        Map<String, Outcome> outcomes = runAndKill("topological", dag, RecordingTopological.class, Map.of());

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
        AgeGraphLoader.byKey(cypher, AgeGraph.named(name), CODEC, SHARDS).create().load(graph, 200);
        String run = name + "-run";
        new TaskRuns(tables, run, "it", Duration.ZERO);   // creates task_run, which the test polls before workers do
        List<GenericContainer<?>> workers = new ArrayList<>();
        for (int w = 0; w < 3; w++) {
            String id = "w" + w;
            GenericContainer<?> worker = new GenericContainer<>(IMAGE);
            worker.setNetwork(NETWORK);
            worker.addEnv("DB_URL", "jdbc:postgresql://" + AgeContainers.ALIAS + ":5432/postgres");
            worker.addEnv("DB_PASSWORD", AgeContainers.PASSWORD);
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
            PostgresClusterStore cluster = new PostgresClusterStore(tables);
            PostgresCompletionLog<String> log = new PostgresCompletionLog<>(tables, run, CODEC);

            waitFor(Duration.ofMinutes(2), "the victim to start a task", () -> !TaskRuns.ranBy(tables, run, VICTIM).isEmpty());
            kill(DockerClientFactory.instance().client(), workers.get(0));
            Set<String> loggedBeforeKill = log.all().keySet();

            waitFor(Duration.ofMinutes(3), "the run to finish",
                    () -> cluster.run(run).map(state -> state.status() != RunStatus.RUNNING).orElse(false));
            ClusterReport report = ClusterReport.read(cluster, run);
            Map<String, Integer> runs = TaskRuns.counts(tables, run);
            Set<String> ranByVictim = TaskRuns.ranBy(tables, run, VICTIM);
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
