package com.salesforce.einstein.tagging.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** End to end over HTTP: default wiring (H2 shard, in-process events, Roaring index, relay on a timer). */
@SpringBootTest(properties = {
        "tagging.tiers[TINY].writes-per-second=2",
        "tagging.tenants[tiny-tenant].tier=TINY",
        "tagging.trending.cache-ttl=0s"})
@AutoConfigureMockMvc
class TaggingApiTest {
    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper json;

    private final String tenant = "t-" + UUID.randomUUID().toString().substring(0, 8);

    private MockHttpServletRequestBuilder as(String tenantId, MockHttpServletRequestBuilder b) {
        return b.header("X-Tenant-Id", tenantId).header("X-Actor-Id", "alice").contentType(MediaType.APPLICATION_JSON);
    }

    private JsonNode body(MvcResult r) throws Exception {
        return json.readTree(r.getResponse().getContentAsString());
    }

    private String createTag(String name) throws Exception {
        MvcResult r = mvc.perform(as(tenant, post("/v1/tags")).content("{\"name\":\"" + name + "\",\"color\":\"#ff0000\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("ETag", "\"v1\""))
                .andExpect(header().exists("X-Consistency-Token"))
                .andReturn();
        JsonNode node = body(r);
        assertThat(node.get("id").isTextual()).as("64-bit ids are strings on the wire").isTrue();
        return node.get("id").asText();
    }

    @Test
    @DisplayName("create → attach → session search sees the write")
    void createAttachSearch() throws Exception {
        String bug = createTag("bug");
        String ui = createTag("ui");
        MvcResult attach = mvc.perform(as(tenant, post("/v1/entities/jira:issue/10042/tags:attach"))
                        .content("{\"tagIds\":[" + bug + "," + ui + "],\"tagNames\":[\"Sprint 42\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tags.length()").value(3))
                .andReturn();
        String token = attach.getResponse().getHeader("X-Consistency-Token");
        assertThat(token).startsWith("s0:");

        mvc.perform(as(tenant, get("/v1/entities/jira:issue/10042/tags")).param("consistency", "session")
                        .header("X-Consistency-Token", token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tags[*].name").value(org.hamcrest.Matchers.contains("bug", "Sprint 42", "ui")));

        mvc.perform(as(tenant, post("/v1/search")).header("X-Consistency-Token", token)
                        .content("{\"all\":[" + bug + "],\"none\":[],\"consistency\":\"SESSION\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].type").value("jira:issue"))
                .andExpect(jsonPath("$.items[0].id").value("10042"));

        mvc.perform(as(tenant, get("/v1/tags/" + bug + "/entities")).param("consistency", "STRONG"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1));

        mvc.perform(as(tenant, put("/v1/entities/jira:issue/10042/tags")).content("{\"tagIds\":[" + ui + "]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tags[*].name").value(org.hamcrest.Matchers.contains("ui")));
    }

    @Test
    @DisplayName("PATCH needs If-Match: 428 without, 412 when stale, 200 with the current ETag")
    void optimisticConcurrency() throws Exception {
        String id = createTag("alpha");
        mvc.perform(as(tenant, patch("/v1/tags/" + id)).content("{\"name\":\"beta\"}"))
                .andExpect(status().isPreconditionRequired());
        mvc.perform(as(tenant, patch("/v1/tags/" + id)).header("If-Match", "\"v1\"").content("{\"name\":\"beta\"}"))
                .andExpect(status().isOk())
                .andExpect(header().string("ETag", "\"v2\""));
        mvc.perform(as(tenant, patch("/v1/tags/" + id)).header("If-Match", "W/\"v1\"").content("{\"name\":\"gamma\"}"))
                .andExpect(status().isPreconditionFailed())
                .andExpect(jsonPath("$.status").value(412));
        mvc.perform(as(tenant, patch("/v1/tags/" + id)).header("If-Match", "*").content("{\"name\":\"gamma\"}"))
                .andExpect(status().isPreconditionRequired());   // a blind PATCH could lose an update
        mvc.perform(as(tenant, get("/v1/tags")).param("prefix", "be"))
                .andExpect(jsonPath("$.items[0].name").value("beta"));
        mvc.perform(as(tenant, delete("/v1/tags/" + id)).header("If-Match", "*"))
                .andExpect(status().isNoContent());               // RFC 9110: "*" matches any current version
    }

    @Test
    @DisplayName("duplicate names conflict; deleted tags vanish; other tenants get 404")
    void conflictsAndIsolation() throws Exception {
        String id = createTag("release");
        mvc.perform(as(tenant, post("/v1/tags")).content("{\"name\":\"RELEASE\"}"))
                .andExpect(status().isConflict());
        mvc.perform(as("someone-else", get("/v1/tags/" + id))).andExpect(status().isNotFound());
        mvc.perform(as(tenant, delete("/v1/tags/" + id))).andExpect(status().isNoContent());
        mvc.perform(as(tenant, get("/v1/tags/" + id))).andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("bad input is a 400 problem; a missing tenant header too")
    void validation() throws Exception {
        mvc.perform(post("/v1/tags").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(as(tenant, post("/v1/tags")).content("{}")).andExpect(status().isBadRequest());
        mvc.perform(as(tenant, get("/v1/entities/NOT_A_TYPE/1/tags"))).andExpect(status().isBadRequest());
        mvc.perform(as(tenant, post("/v1/tags:bulkAttach"))
                        .content("{\"entities\":[{\"type\":\"jira:issue\",\"id\":\"1\"}],\"tagIds\":[1]}"))
                .andExpect(status().isBadRequest());   // Idempotency-Key is required
    }

    @Test
    @DisplayName("bulk attach is idempotent per Idempotency-Key")
    void bulkAttach() throws Exception {
        String id = createTag("imported");
        String req = "{\"entities\":[{\"type\":\"jira:issue\",\"id\":\"1\"},{\"type\":\"jira:issue\",\"id\":\"2\"}],"
                + "\"tagIds\":[" + id + "]}";
        for (int i = 0; i < 2; i++) {
            mvc.perform(as(tenant, post("/v1/tags:bulkAttach")).header("Idempotency-Key", "import-batch-1").content(req))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.attached").value(2));
        }
        mvc.perform(as(tenant, get("/v1/tags/" + id)).param("consistency", "strong"))
                .andExpect(jsonPath("$.usage").value(2));
    }

    @Test
    @DisplayName("trending: attaches show up after the relay, marked eventual; bad params are 400")
    void trending() throws Exception {
        String hot = createTag("hot");
        String warm = createTag("warm");
        for (int i = 0; i < 3; i++) {
            mvc.perform(as(tenant, post("/v1/entities/jira:issue/t" + i + "/tags:attach"))
                    .content("{\"tagIds\":[" + hot + (i == 0 ? "," + warm : "") + "]}")).andExpect(status().isOk());
        }
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                mvc.perform(as(tenant, get("/v1/tags:trending")).param("window", "24h").param("limit", "5"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.consistency").value("EVENTUAL"))
                        .andExpect(jsonPath("$.window").value("24h"))
                        .andExpect(jsonPath("$.items[*].tag.name").value(org.hamcrest.Matchers.contains("hot", "warm")))
                        .andExpect(jsonPath("$.items[0].tag.id").value(hot))
                        .andExpect(jsonPath("$.items[0].count").value(3)));
        mvc.perform(as(tenant, get("/v1/tags:trending")).param("window", "7d").param("rank", "rising"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(2));
        mvc.perform(as("someone-else", get("/v1/tags:trending"))).andExpect(jsonPath("$.items.length()").value(0));
        mvc.perform(as(tenant, get("/v1/tags:trending")).param("window", "1y")).andExpect(status().isBadRequest());
        mvc.perform(as(tenant, get("/v1/tags:trending")).param("rank", "best")).andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("a tenant over its write quota gets 429 with Retry-After; others are unaffected")
    void rateLimited() throws Exception {
        int limited = 0;
        for (int i = 0; i < 5; i++) {
            int s = mvc.perform(as("tiny-tenant", post("/v1/tags")).content("{\"name\":\"n" + UUID.randomUUID() + "\"}"))
                    .andReturn().getResponse().getStatus();
            if (s == 429) {
                limited++;
            }
        }
        assertThat(limited).isGreaterThanOrEqualTo(2);
        mvc.perform(as("tiny-tenant", post("/v1/tags")).content("{\"name\":\"late\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"));
        createTag("unaffected");
    }
}
