package com.salesforce.einstein.webcrawler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"crawler.politeness-delay=0s", "crawler.retry-backoff=1ms", "crawler.workers=4"})
@AutoConfigureMockMvc
class SitemapGraphControllerTest {

    static final String S = "https://graphs-test.com";
    static final String NAV = S + "/nav.xml";

    @TestConfiguration
    static class Web {
        @Bean @Primary Fetcher fakeWeb() {
            return new FakeWeb()
                    .page(S + "/", "<a href='/a'>a</a>")
                    .page(S + "/a", "<p>a</p>")
                    .asset(NAV, "application/xml", ("<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'"
                            + " xmlns:nav='urn:webcrawler:sitemap-nav'><url nav:root='true'><loc>" + S + "/</loc>"
                            + "<nav:link href='/a'/></url></urlset>").getBytes(StandardCharsets.UTF_8));
        }
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;

    private ResultActions load(String tenant, String name, String sitemapUrl) throws Exception {
        return mvc.perform(put("/v1/sitemap-graphs/" + name).header("X-Tenant-Id", tenant)
                .contentType(MediaType.APPLICATION_JSON).content("{\"sitemapUrl\":\"" + sitemapUrl + "\"}"));
    }

    /** {@code t1}'s view of the graph. */
    private JsonNode view(String name) throws Exception {
        return json.readTree(mvc.perform(get("/v1/sitemap-graphs/" + name).header("X-Tenant-Id", "t1"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    }

    /** {@code t1}'s view of the graph once its load has ended. */
    private JsonNode finished(String name) {
        return await("the load ends").atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                .until(() -> view(name), v -> !v.get("status").asText().equals("LOADING"));
    }

    @Test void aGraphIsLoadedPolledCrawledAndDeleted() throws Exception {
        load("t1", "api_nav", NAV)
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/v1/sitemap-graphs/api_nav"))
                .andExpect(jsonPath("$.status").value("LOADING"))
                .andExpect(jsonPath("$.roots").doesNotExist());

        JsonNode ready = finished("api_nav");
        assertThat(ready.get("status").asText()).isEqualTo("READY");
        mvc.perform(get("/v1/sitemap-graphs/api_nav").header("X-Tenant-Id", "t1"))
                .andExpect(jsonPath("$.pages").value(2))
                .andExpect(jsonPath("$.edges").value(1))
                .andExpect(jsonPath("$.roots[0]").value(S + "/"))
                .andExpect(jsonPath("$.rootCount").value(1))
                .andExpect(jsonPath("$.links.self").value("/v1/sitemap-graphs/api_nav"));

        load("t1", "api_nav", NAV).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("READY"));
        load("t1", "api_nav", S + "/other.xml").andExpect(status().isConflict());
        load("t2", "api_nav", NAV).andExpect(status().isConflict());
        mvc.perform(get("/v1/sitemap-graphs/api_nav").header("X-Tenant-Id", "t2")).andExpect(status().isNotFound());
        mvc.perform(delete("/v1/sitemap-graphs/api_nav").header("X-Tenant-Id", "t2")).andExpect(status().isNotFound());

        mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seeds\":[\"" + S + "/\"],\"sitemapGraph\":\"api_nav\",\"respectRobots\":false}"))
                .andExpect(status().isAccepted());
        mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t2").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seeds\":[\"" + S + "/\"],\"sitemapGraph\":\"api_nav\"}"))
                .andExpect(status().isBadRequest());

        mvc.perform(delete("/v1/sitemap-graphs/api_nav").header("X-Tenant-Id", "t1")).andExpect(status().isNoContent());
        mvc.perform(get("/v1/sitemap-graphs/api_nav").header("X-Tenant-Id", "t1")).andExpect(status().isNotFound());
        mvc.perform(delete("/v1/sitemap-graphs/api_nav").header("X-Tenant-Id", "t1")).andExpect(status().isNotFound());
    }

    @Test void aSitemapThatCannotBeUsedEndsFailed() throws Exception {
        load("t1", "missing_nav", S + "/missing.xml").andExpect(status().isAccepted());
        JsonNode failed = finished("missing_nav");
        assertThat(failed.get("status").asText()).isEqualTo("FAILED");
        assertThat(failed.get("error").asText()).contains("HTTP 404");
    }

    @Test void badRequestsAreRejected() throws Exception {
        load("t1", "api_nav", "ftp://graphs-test.com/nav.xml").andExpect(status().isBadRequest());
        load("t1", "sitemap_x", NAV).andExpect(status().isBadRequest());
        load("t1", "no", NAV).andExpect(status().isBadRequest());
        mvc.perform(put("/v1/sitemap-graphs/api_nav").contentType(MediaType.APPLICATION_JSON)
                .content("{\"sitemapUrl\":\"" + NAV + "\"}")).andExpect(status().isBadRequest());   // no tenant
    }
}
