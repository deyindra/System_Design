package com.salesforce.einstein.hierarchy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.spi.TreeEventListener;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The whole application over HTTP: the configuration wires one shard, the relay, the worker and the controllers. The
 * context is closed afterward, so its scheduled relay and worker don't drain the shared database under later ITs.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ApiIT {
    private static final String TOKEN = "X-Read-Token";
    private static final String IDEMPOTENCY = "Idempotency-Key";

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("hierarchy.shards.s0.url", Pg::jdbcUrl);
        r.add("hierarchy.shards.s0.username", Pg::username);
        r.add("hierarchy.shards.s0.password", Pg::password);
        r.add("hierarchy.move.sync-limit", () -> "3");
        r.add("hierarchy.move.batch-size", () -> "2");
        r.add("hierarchy.move.sweep-interval-ms", () -> "100");
    }

    @TestConfiguration
    static class Listener {
        @Bean
        Recorder recorder() {
            return new Recorder();
        }
    }

    static final class Recorder implements TreeEventListener {
        final List<TreeEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void onEvents(List<TreeEvent> batch) {
            events.addAll(batch);
        }
    }

    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper json;
    @Autowired
    Recorder recorder;

    private final String tenant = UUID.randomUUID().toString();

    @Test
    void buildMoveAndReadATree() throws Exception {
        JsonNode space = body(send(post("/v1/spaces", Map.of("key", "DOCS", "title", "Docs")), 201));
        String spaceId = space.get("id").asText();
        String root = space.get("rootNodeId").asText();

        MvcResult created = send(post("/v1/spaces/" + spaceId + "/nodes",
                Map.of("parentId", root, "title", "Guides", "type", "page")), 201);
        assertThat(created.getResponse().getHeader("ETag")).isEqualTo("\"v1\"");
        assertThat(created.getResponse().getHeader(TOKEN)).startsWith("s0:");
        String guides = body(created).get("id").asText();
        String setup = node(spaceId, guides, "Setup");
        String other = node(spaceId, root, "Other");

        JsonNode children = body(send(get("/v1/nodes/" + root + "/children?limit=1"), 200));
        assertThat(children.get("items").get(0).get("id").asText()).isEqualTo(guides);
        String cursor = children.get("nextCursor").asText();
        assertThat(body(send(get("/v1/nodes/" + root + "/children?limit=1&cursor=" + cursor), 200))
                .get("items").get(0).get("id").asText()).isEqualTo(other);

        JsonNode moved = body(send(post("/v1/nodes/" + guides + "/move", Map.of("newParentId", other)), 200));
        assertThat(moved.get("node").get("path").asText()).isEqualTo("/" + root + "/" + other + "/" + guides + "/");
        assertThat(moved.has("job")).isFalse();

        JsonNode crumbs = body(send(get("/v1/nodes/" + setup + "/ancestors"), 200));
        assertThat(crumbs).extracting(c -> c.get("title").asText()).containsExactly("Docs", "Other", "Guides");
        JsonNode subtree = body(send(get("/v1/nodes/" + root + "/descendants?depth=5"), 200));
        assertThat(subtree.get("items")).extracting(n -> n.get("id").asText()).containsExactly(other, guides, setup);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(recorder.events)
                .filteredOn(e -> e.tenantId().toString().equals(tenant)).extracting(TreeEvent::type)
                .containsExactly(TreeEvent.Type.SPACE_CREATED, TreeEvent.Type.NODE_CREATED,
                        TreeEvent.Type.NODE_CREATED, TreeEvent.Type.NODE_CREATED, TreeEvent.Type.NODE_MOVED));
    }

    @Test
    void optimisticConcurrencyAndValidation() throws Exception {
        JsonNode space = body(send(post("/v1/spaces", Map.of("key", "OCC", "title", "Occ")), 201));
        String spaceId = space.get("id").asText();
        String page = node(spaceId, space.get("rootNodeId").asText(), "Draft");

        send(patch("/v1/nodes/" + page, Map.of("title", "Final")), 428);
        MvcResult ok = send(patch("/v1/nodes/" + page, Map.of("title", "Final", "body", "text")).header("If-Match",
                "\"v1\""), 200);
        assertThat(ok.getResponse().getHeader("ETag")).isEqualTo("\"v2\"");
        send(patch("/v1/nodes/" + page, Map.of("title", "Again")).header("If-Match", "\"v1\""), 412);

        // Read your write through the token, then with the body.
        String token = ok.getResponse().getHeader(TOKEN);
        JsonNode read = body(send(get("/v1/nodes/" + page + "?body=true").header(TOKEN, token), 200));
        assertThat(read.get("title").asText()).isEqualTo("Final");
        assertThat(read.get("body").asText()).isEqualTo("text");
        assertThat(read.get("inTrash").asBoolean()).isFalse();

        send(get("/v1/nodes/123"), 404);
        send(post("/v1/spaces/" + spaceId + "/nodes", Map.of("parentId", "x", "title", "t", "type", "page")), 400);
        send(post("/v1/spaces", Map.of("key", "OCC", "title", "dup")), 409);
        mvc.perform(MockMvcRequestBuilders.get("/v1/nodes/1")).andExpect(status().isBadRequest());   // no tenant
    }

    @Test
    void idempotentCreate() throws Exception {
        JsonNode space = body(send(post("/v1/spaces", Map.of("key", "IDEM", "title", "Idem")), 201));
        String path = "/v1/spaces/" + space.get("id").asText() + "/nodes";
        Map<String, String> req = Map.of("parentId", space.get("rootNodeId").asText(), "title", "Once", "type", "page");

        String first = body(send(post(path, req).header(IDEMPOTENCY, "req-000001"), 201)).get("id").asText();
        String again = body(send(post(path, req).header(IDEMPOTENCY, "req-000001"), 201)).get("id").asText();
        assertThat(again).isEqualTo(first);
        send(post(path, Map.of("parentId", req.get("parentId"), "title", "Other", "type", "page"))
                .header(IDEMPOTENCY, "req-000001"), 422);
        send(post(path, req).header(IDEMPOTENCY, "bad key!"), 400);
    }

    @Test
    void largeMoveIsAcceptedAndTheWorkerFinishesIt() throws Exception {
        JsonNode space = body(send(post("/v1/spaces", Map.of("key", "LARGE", "title", "Large")), 201));
        String spaceId = space.get("id").asText();
        String root = space.get("rootNodeId").asText();
        String big = node(spaceId, root, "Big");
        for (int i = 0; i < 5; i++) {
            node(spaceId, big, "child " + i);
        }
        String target = node(spaceId, root, "Target");

        MvcResult accepted = mvc.perform(caller(MockMvcRequestBuilders.post("/v1/nodes/" + big + "/move"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("newParentId", target))))
                .andExpect(status().isAccepted()).andExpect(header().exists(TOKEN)).andReturn();
        JsonNode job = body(accepted).get("job");
        assertThat(job.get("state").asText()).isEqualTo("RUNNING");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JsonNode j = body(send(get("/v1/move-jobs/" + job.get("id").asText()), 200));
            assertThat(j.get("state").asText()).isEqualTo("DONE");
            assertThat(j.get("rowsDone").asLong()).isEqualTo(5);
        });
        JsonNode subtree = body(send(get("/v1/nodes/" + target + "/descendants?depth=2"), 200));
        assertThat(subtree.get("items")).hasSize(6)
                .allSatisfy(n -> assertThat(n.get("path").asText()).contains("/" + target + "/" + big + "/"));
        assertThat(body(send(get("/actuator/health"), 200)).get("status").asText()).isEqualTo("UP");
    }

    @Test
    void trashRestrictAndPurge() throws Exception {
        JsonNode space = body(send(post("/v1/spaces", Map.of("key", "TRASH", "title", "Trash")), 201));
        String spaceId = space.get("id").asText();
        String page = node(spaceId, space.get("rootNodeId").asText(), "Old");

        send(put("/v1/nodes/" + page + "/restrictions", Map.of("op", "view", "principals", List.of("group:eng"))), 200);
        mvc.perform(MockMvcRequestBuilders.get("/v1/nodes/" + page).header("X-Tenant-Id", tenant)
                .header("X-Actor-Id", "mallory")).andExpect(status().isNotFound());

        MvcResult trashed = send(post("/v1/nodes/" + page + "/trash", Map.of()).header("If-Match", "\"v1\""), 200);
        assertThat(body(send(get("/v1/spaces/" + spaceId + "/trash"), 200))).extracting(n -> n.get("id").asText())
                .containsExactly(page);
        JsonNode purged = body(send(post("/v1/nodes/" + page + "/purge", Map.of())
                .header("If-Match", trashed.getResponse().getHeader("ETag")), 200));
        assertThat(purged.get("purged").asLong()).isEqualTo(1);
        send(get("/v1/nodes/" + page), 404);
    }

    private String node(String spaceId, String parentId, String title) throws Exception {
        return body(send(post("/v1/spaces/" + spaceId + "/nodes",
                Map.of("parentId", parentId, "title", title, "type", "page")), 201)).get("id").asText();
    }

    private MockHttpServletRequestBuilder get(String path) {
        return caller(MockMvcRequestBuilders.get(path));
    }

    private MockHttpServletRequestBuilder post(String path, Object body) throws Exception {
        return withBody(MockMvcRequestBuilders.post(path), body);
    }

    private MockHttpServletRequestBuilder patch(String path, Object body) throws Exception {
        return withBody(MockMvcRequestBuilders.patch(path), body);
    }

    private MockHttpServletRequestBuilder put(String path, Object body) throws Exception {
        return withBody(MockMvcRequestBuilders.put(path), body);
    }

    private MockHttpServletRequestBuilder withBody(MockHttpServletRequestBuilder b, Object body) throws Exception {
        return caller(b).contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(body));
    }

    private MockHttpServletRequestBuilder caller(MockHttpServletRequestBuilder b) {
        return b.header("X-Tenant-Id", tenant).header("X-Actor-Id", "alice").header("X-Actor-Groups", "eng");
    }

    private MvcResult send(MockHttpServletRequestBuilder req, int expectedStatus) throws Exception {
        return mvc.perform(req).andExpect(status().is(expectedStatus)).andReturn();
    }

    private JsonNode body(MvcResult r) throws Exception {
        return json.readTree(r.getResponse().getContentAsString());
    }
}
