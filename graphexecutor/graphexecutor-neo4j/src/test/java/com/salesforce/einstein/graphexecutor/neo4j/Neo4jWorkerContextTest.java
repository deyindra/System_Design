package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.graphexecutor.distributed.Worker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The environment a worker reads. The driver connects on first use, so no database is needed here. */
class Neo4jWorkerContextTest {

    private static final Map<String, String> REQUIRED =
            Map.of("NEO4J_URI", "bolt://localhost:7687", "SHARDS", "8", "RUN_ID", "run-1");

    private static Map<String, String> with(String... pairs) {
        Map<String, String> env = new HashMap<>(REQUIRED);
        for (int i = 0; i < pairs.length; i += 2) {
            env.put(pairs[i], pairs[i + 1]);
        }
        return env;
    }

    @Test
    void defaultsEverythingButTheRequiredSettings() {
        try (Neo4jWorkerContext context = Neo4jWorkerContext.of(with("HOSTNAME", "pod-7"))) {
            assertEquals(8, context.shards());
            assertEquals(Neo4jGraph.named("Node"), context.graph());
            assertEquals(Worker.Config.of("run-1", "pod-7"), context.config());
            assertEquals("fallback", context.env("TASK_TIME", "fallback"));
        }
    }

    @Test
    void readsEveryOptionalSetting() {
        Map<String, String> env = with("GRAPH", "Page", "WORKER_ID", "w3", "HOSTNAME", "pod-7",
                "SHARDS_PER_WORKER", "2", "LEASE_TTL", "PT3S", "POLL_INTERVAL", "PT0.05S", "BATCH_SIZE", "50",
                "TASK_TIME", "PT1S");
        try (Neo4jWorkerContext context = Neo4jWorkerContext.of(env)) {
            assertEquals(Neo4jGraph.named("Page"), context.graph());
            assertEquals(new Worker.Config("run-1", "w3", 2, Duration.ofSeconds(3), Duration.ofMillis(50), 50),
                    context.config());
            assertEquals("PT1S", context.env("TASK_TIME", "fallback"));
        }
    }

    @Test
    void aWorkerWithoutAnIdOrHostnameGetsARandomOne() {
        try (Neo4jWorkerContext one = Neo4jWorkerContext.of(REQUIRED);
             Neo4jWorkerContext two = Neo4jWorkerContext.of(REQUIRED)) {
            assertFalse(one.config().workerId().isBlank());
            assertNotEquals(one.config().workerId(), two.config().workerId(), "unique per worker");
        }
    }

    @Test
    void theEnvironmentIsCopied() {
        Map<String, String> env = with("GRAPH", "Page");
        try (Neo4jWorkerContext context = Neo4jWorkerContext.of(env)) {
            env.put("TASK_TIME", "PT1S");
            assertEquals("fallback", context.env("TASK_TIME", "fallback"));
        }
    }

    @Test
    void rejectsAMissingOrBlankRequiredSetting() {
        for (String name : REQUIRED.keySet()) {
            Map<String, String> missing = new HashMap<>(REQUIRED);
            missing.remove(name);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> Neo4jWorkerContext.of(missing).close(), name);
            assertTrue(e.getMessage().contains(name), e.getMessage());
            assertThrows(IllegalArgumentException.class, () -> Neo4jWorkerContext.of(with(name, " ")).close(), name);
        }
    }

    @Test
    void rejectsAGraphNameThatIsNotAnIdentifier() {
        assertThrows(IllegalArgumentException.class, () -> Neo4jWorkerContext.of(with("GRAPH", "my graph")).close());
    }
}
