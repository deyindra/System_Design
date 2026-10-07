package com.salesforce.einstein.graphexecutor.neo4j.demo;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jGraph;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jGraphLoader;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jSessions;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jWorkerContext;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import org.neo4j.driver.Driver;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Random;

/**
 * A reproducible random graph, and a main that loads one into Neo4j (the demo's loader job), logging its size.
 * Nodes are {@code n0..n<NODES-1>}; with {@code ACYCLIC}, edges only go from a lower to a higher index. The same seed
 * gives the same graph as the AGE module's {@code RandomGraph}.
 * <pre>NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE, GRAPH, SHARDS
 *                                as for {@link com.salesforce.einstein.graphexecutor.neo4j.Neo4jWorkerContext}
 * NODES, EDGES, SEED, ACYCLIC    (1000, 3000, 1, false)</pre>
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
        try (Driver driver = Neo4jWorkerContext.driver(env)) {
            Neo4jSessions sessions = new Neo4jSessions(driver, env.getOrDefault("NEO4J_DATABASE", "neo4j"));
            Neo4jGraphLoader.byKey(sessions, Neo4jGraph.named(env.getOrDefault("GRAPH", "Node")), NodeCodec.strings(),
                    Integer.parseInt(Neo4jWorkerContext.require(env, "SHARDS"))).create().load(graph, 500);
        }
        LOG.log(Level.INFO, () -> "loaded " + graph.nodeCount() + " nodes, " + graph.edgeCount() + " edges");
    }
}
