package com.salesforce.einstein.graphexecutor.neo4j;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Where a graph is in Neo4j: the label of its nodes and the type of its relationships (one database can hold
 * many graphs, under different labels). Each node has a {@code key} (the node, as its
 * {@link com.salesforce.einstein.graphexecutor.spi.NodeCodec} writes it) and a {@code shard} property.
 *
 * <p>Labels go into Cypher text (Cypher takes no parameters for them), so they must be plain identifiers.
 */
public record Neo4jGraph(String nodeLabel, String edgeType) {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public Neo4jGraph {
        check(nodeLabel, "nodeLabel");
        check(edgeType, "edgeType");
    }

    /** Nodes labelled {@code <name>}, relationships typed {@code <name>_EDGE}. */
    public static Neo4jGraph named(String name) {
        return new Neo4jGraph(name, name + "_EDGE");
    }

    private static void check(String identifier, String what) {
        Objects.requireNonNull(identifier, what);
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(what + " must be an identifier: " + identifier);
        }
    }
}
