package com.salesforce.einstein.graphexecutor.neo4j;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Neo4jGraphTest {

    @Test
    void namedDerivesTheRelationshipType() {
        assertEquals(new Neo4jGraph("Page", "Page_EDGE"), Neo4jGraph.named("Page"));
    }

    @Test
    void acceptsPlainIdentifiers() {
        for (String label : List.of("Node", "_private", "n0", "Snake_Case_2")) {
            assertEquals(label, Neo4jGraph.named(label).nodeLabel());
        }
    }

    /** Labels are spliced into Cypher text, so anything that could end the label is rejected. */
    @Test
    void rejectsWhatIsNotAnIdentifier() {
        for (String label : List.of("", "2nodes", "two words", "a-b", "a`b", "Node) DETACH DELETE (x", "Nöde")) {
            assertThrows(IllegalArgumentException.class, () -> Neo4jGraph.named(label), label);
        }
        assertThrows(IllegalArgumentException.class, () -> new Neo4jGraph("Node", "NEXT;"));
        assertThrows(NullPointerException.class, () -> new Neo4jGraph(null, "NEXT"));
        assertThrows(NullPointerException.class, () -> new Neo4jGraph("Node", null));
    }
}
