package com.salesforce.einstein.graphexecutor.age;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AgtypeTest {

    @Test
    void writesParameterMapsAsJson() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("shard", 3);
        params.put("keys", List.of("a", "b\"c"));
        params.put("edges", List.of(List.of("x", "y")));

        assertEquals("{\"shard\":3,\"keys\":[\"a\",\"b\\\"c\"],\"edges\":[[\"x\",\"y\"]]}", Agtype.json(params));
    }

    @Test
    void stringsSurviveTheRoundTrip() {
        for (String key : List.of("", "plain", "q\"uote", "back\\slash", "tab\tnew\nline", "bell\u0007", "ünï")) {
            assertEquals(key, Agtype.string(Agtype.json(key)), key);
        }
        assertEquals("/", Agtype.string("\"\\/\""));
        assertNull(Agtype.string(null));
        assertNull(Agtype.string("null"));
    }

    @Test
    void rejectsWhatIsNotAString() {
        assertThrows(IllegalArgumentException.class, () -> Agtype.string("42"));
        assertThrows(IllegalArgumentException.class, () -> Agtype.json(Map.of("x", 1.5)));
    }

    @Test
    void graphNamesMustBeIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> AgeGraph.named("g"));
        assertThrows(IllegalArgumentException.class, () -> AgeGraph.named("bad'name"));
        assertEquals("SELECT * FROM cypher('demo', $$ RETURN 1 $$, ?) AS (x agtype)",
                AgeGraph.named("demo").cypher("RETURN 1", "x agtype"));
    }
}
