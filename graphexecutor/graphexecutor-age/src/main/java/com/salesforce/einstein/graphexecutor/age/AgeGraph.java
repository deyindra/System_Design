package com.salesforce.einstein.graphexecutor.age;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Where a graph is in AGE: the graph's name and the labels of its vertices and edges. Each vertex has a
 * {@code key} (the node, as its {@link com.salesforce.einstein.graphexecutor.spi.NodeCodec} writes it) and a
 * {@code shard} property.
 *
 * <p>Names go into SQL and Cypher text (AGE takes no parameters for them), so they must be plain identifiers.
 * AGE also wants graph names of at least 3 characters.
 */
public record AgeGraph(String name, String vertexLabel, String edgeLabel) {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public AgeGraph {
        check(name, "name");
        check(vertexLabel, "vertexLabel");
        check(edgeLabel, "edgeLabel");
        if (name.length() < 3) {
            throw new IllegalArgumentException("AGE graph names have at least 3 characters: " + name);
        }
    }

    /** Vertices labelled {@code V}, edges {@code E}. */
    public static AgeGraph named(String name) {
        return new AgeGraph(name, "V", "E");
    }

    /** {@code SELECT * FROM cypher('<graph>', $$ <cypher> $$, ?) AS (<columns>)}: a Cypher query with one agtype parameter map. */
    String cypher(String cypher, String columns) {
        return "SELECT * FROM cypher('" + name + "', $$ " + cypher + " $$, ?) AS (" + columns + ")";
    }

    /** The table AGE keeps a label's rows in. */
    String table(String label) {
        return '"' + name + "\".\"" + label + '"';
    }

    private static void check(String identifier, String what) {
        Objects.requireNonNull(identifier, what);
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(what + " must be an identifier: " + identifier);
        }
    }
}
