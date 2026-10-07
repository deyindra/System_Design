package com.salesforce.einstein.graphexecutor.age.demo;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.age.AgeGraph;
import com.salesforce.einstein.graphexecutor.age.AgeGraphLoader;
import com.salesforce.einstein.graphexecutor.age.AgeWorkerContext;
import com.salesforce.einstein.graphexecutor.age.PgConnections;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Random;

/**
 * A reproducible random graph, and a main that loads one into AGE (the demo's loader job), logging its size.
 * Nodes are {@code n0..n<NODES-1>}; with {@code ACYCLIC}, edges only go from a lower to a higher index.
 * <pre>DB_URL, DB_USER, DB_PASSWORD, GRAPH, SHARDS   as for {@link com.salesforce.einstein.graphexecutor.age.AgeWorkerContext}
 * NODES, EDGES, SEED, ACYCLIC                    (1000, 3000, 1, false)</pre>
 */
public final class RandomGraph {

    private static final Logger LOG = System.getLogger(RandomGraph.class.getName());

    private RandomGraph() {
    }

    public static Graph<String> generate(long seed, int nodes, int edges, boolean acyclic) {
        Random random = new Random(seed);
        Graph<String> graph = Graph.directed();
        for (int i = 0; i < nodes; i++) {
            graph.addNode("n" + i);
        }
        while (graph.edgeCount() < edges) {
            int a = random.nextInt(nodes);
            int b = random.nextInt(nodes);
            if (a != b && (!acyclic || a < b)) {
                graph.addEdge("n" + a, "n" + b);
            }
        }
        return graph;
    }

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();
        Graph<String> graph = generate(Long.parseLong(env.getOrDefault("SEED", "1")),
                Integer.parseInt(env.getOrDefault("NODES", "1000")), Integer.parseInt(env.getOrDefault("EDGES", "3000")),
                Boolean.parseBoolean(env.getOrDefault("ACYCLIC", "false")));
        try (PgConnections connections = new PgConnections(AgeWorkerContext.require(env, "DB_URL"),
                env.getOrDefault("DB_USER", "postgres"), env.getOrDefault("DB_PASSWORD", ""), 1, PgConnections.AGE)) {
            AgeGraphLoader.byKey(connections, AgeGraph.named(env.getOrDefault("GRAPH", "graph")), NodeCodec.strings(),
                    Integer.parseInt(AgeWorkerContext.require(env, "SHARDS"))).create().load(graph, 500);
        }
        LOG.log(Level.INFO, () -> "loaded " + graph.nodeCount() + " nodes, " + graph.edgeCount() + " edges");
    }
}
