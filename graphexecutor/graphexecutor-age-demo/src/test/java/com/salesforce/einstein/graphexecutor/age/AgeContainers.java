package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.ds.graph.Graph;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The containers the ITs share, and the expected results they check against. */
final class AgeContainers {
    static final String ALIAS = "age";
    static final String PASSWORD = "postgres";
    /** Where the traversal ITs start. */
    static final String ROOT = "n0";

    private AgeContainers() {
    }

    static GenericContainer<?> age(Network network) {
        GenericContainer<?> age = new GenericContainer<>("apache/age:release_PG16_1.5.0");
        age.setNetwork(network);
        age.setNetworkAliases(List.of(ALIAS));
        age.addEnv("POSTGRES_PASSWORD", PASSWORD);
        age.setExposedPorts(List.of(5432));
        // the image's entrypoint starts Postgres once to initialize, then for real
        age.setWaitStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
                .withStartupTimeout(Duration.ofMinutes(2)));
        return age;
    }

    static String url(GenericContainer<?> age) {
        return "jdbc:postgresql://" + age.getHost() + ":" + age.getMappedPort(5432) + "/postgres";
    }

    static PgConnections connections(GenericContainer<?> age, List<String> init) {
        return new PgConnections(url(age), "postgres", PASSWORD, 8, init);
    }

    /** The worker image, from the module's Dockerfile and what {@code package} put in target/. */
    static ImageFromDockerfile workerImage() {
        return new ImageFromDockerfile("graphexecutor-age-demo-worker-it")
                // at the context root as "Dockerfile", where docker build looks for it by default
                .withFileFromPath("Dockerfile", Path.of("src/main/docker/Dockerfile"))
                .withFileFromPath("target/lib", Path.of("target/lib"))
                .withFileFromPath("target/graphexecutor-age-demo.jar", Path.of("target/graphexecutor-age-demo.jar"));
    }

    /** Breadth-first depth from {@link #ROOT}: what a traversal logs as each node's round. */
    static Map<String, Integer> depths(Graph<String> graph) {
        Map<String, Integer> depth = new HashMap<>(Map.of(ROOT, 0));
        Deque<String> queue = new ArrayDeque<>(List.of(ROOT));
        while (!queue.isEmpty()) {
            String node = queue.poll();
            for (String next : graph.successors(node)) {
                if (depth.putIfAbsent(next, depth.get(node) + 1) == null) {
                    queue.add(next);
                }
            }
        }
        return depth;
    }

    /** Longest path from a source, in a DAG: the round a dependency-ordered run runs each node in. */
    static Map<String, Integer> levels(Graph<String> dag) {
        Map<String, Integer> level = new HashMap<>();
        Map<String, Integer> pending = new HashMap<>();
        Deque<String> ready = new ArrayDeque<>();
        for (String node : dag.nodes()) {
            pending.put(node, dag.predecessors(node).size());
            if (dag.predecessors(node).isEmpty()) {
                level.put(node, 0);
                ready.add(node);
            }
        }
        while (!ready.isEmpty()) {
            String node = ready.poll();
            for (String next : dag.successors(node)) {
                level.merge(next, level.get(node) + 1, Math::max);
                if (pending.merge(next, -1, Integer::sum) == 0) {
                    ready.add(next);
                }
            }
        }
        return level;
    }
}
