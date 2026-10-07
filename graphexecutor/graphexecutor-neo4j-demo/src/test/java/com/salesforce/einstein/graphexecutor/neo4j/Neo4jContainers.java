package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.ds.graph.Graph;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
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
final class Neo4jContainers {
    static final String ALIAS = "neo4j";
    static final String PASSWORD = "graphexecutor";
    /** Where the traversal ITs start. */
    static final String ROOT = "n0";

    private Neo4jContainers() {
    }

    static GenericContainer<?> neo4j(Network network) {
        GenericContainer<?> neo4j = new GenericContainer<>("neo4j:5");
        neo4j.setNetwork(network);
        neo4j.setNetworkAliases(List.of(ALIAS));
        neo4j.addEnv("NEO4J_AUTH", "neo4j/" + PASSWORD);
        neo4j.setExposedPorts(List.of(7687));
        neo4j.setWaitStrategy(Wait.forLogMessage(".*Started\\..*\\s", 1).withStartupTimeout(Duration.ofMinutes(2)));
        return neo4j;
    }

    static Driver driver(GenericContainer<?> neo4j) {
        return GraphDatabase.driver("bolt://" + neo4j.getHost() + ":" + neo4j.getMappedPort(7687),
                AuthTokens.basic("neo4j", PASSWORD));
    }

    /** The worker image, from the module's Dockerfile and what {@code package} put in target/. */
    static ImageFromDockerfile workerImage() {
        return new ImageFromDockerfile("graphexecutor-neo4j-demo-worker-it")
                // at the context root as "Dockerfile", where docker build looks for it by default
                .withFileFromPath("Dockerfile", Path.of("src/main/docker/Dockerfile"))
                .withFileFromPath("target/lib", Path.of("target/lib"))
                .withFileFromPath("target/graphexecutor-neo4j-demo.jar", Path.of("target/graphexecutor-neo4j-demo.jar"));
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
