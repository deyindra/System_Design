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
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"crawler.politeness-delay=0s", "crawler.retry-backoff=1ms", "crawler.workers=4"})
@AutoConfigureMockMvc
class CrawlControllerTest {

    static final String S = "https://api-test.com";

    @TestConfiguration
    static class Web {
        @Bean @Primary Fetcher fakeWeb() {
            return new FakeWeb()
                    .page(S + "/", "<a href='/a'>a</a><a href='/b?utm_source=x'>b</a><img src='/logo.png'>")
                    .page(S + "/a", "<a href='/'>home</a>")
                    .page(S + "/b", "<p>b</p>")
                    .asset(S + "/logo.png", "image/png", new byte[]{1, 2, 3});
        }
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;

    private JsonNode body(MvcResult r) throws Exception { return json.readTree(r.getResponse().getContentAsString()); }

    @Test void syncCrawlReturnsTheWholeGraph() throws Exception {
        MvcResult r = mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seeds\":[\"" + S + "/\"],\"maxDepth\":1,\"maxPages\":10,\"mode\":\"SYNC\"}"))
                .andExpect(status().isOk())
                .andExpect(header().string("Location", startsWith("/v1/crawls/")))
                .andExpect(jsonPath("$.job.status").value("COMPLETED"))
                .andExpect(jsonPath("$.job.stats.pages").value(3))
                .andExpect(jsonPath("$.job.stats.assets").value(1))
                .andReturn();
        assertEquals(4, body(r).get("pages").size());

        String jobId = body(r).get("job").get("jobId").asText();
        String seedHash = body(r).get("pages").get(0).get("urlHash").asText();
        mvc.perform(get("/v1/crawls/" + jobId + "/pages/" + seedHash + "/content").header("X-Tenant-Id", "t1"))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Security-Policy", "sandbox"))
                .andExpect(content().string(containsString("/v1/crawls/" + jobId + "/pages/")));
        mvc.perform(get("/v1/crawls/" + jobId + "/pages/" + seedHash).header("X-Tenant-Id", "t1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outLinks.length()").value(3))
                .andExpect(jsonPath("$.outLinks[1].toUrl").value(S + "/b"))
                .andExpect(jsonPath("$.outLinks[1].rawHref").value("/b?utm_source=x"));
        mvc.perform(get("/v1/crawls/" + jobId + "/export").header("X-Tenant-Id", "t1"))
                .andExpect(status().isOk());
        mvc.perform(get("/v1/pages").param("url", S + "/a?utm_campaign=z").header("X-Tenant-Id", "t1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.url").value(S + "/a"))
                .andExpect(jsonPath("$.jobId").value(jobId))
                .andExpect(jsonPath("$.content", startsWith("/v1/crawls/" + jobId + "/pages/")));
        mvc.perform(get("/v1/pages").param("url", S + "/a").header("X-Tenant-Id", "someone-else"))
                .andExpect(status().isNotFound());   // another tenant's crawl is invisible
    }

    @Test void asyncCrawlIsAcceptedAndPollable() throws Exception {
        MvcResult r = mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").header("Idempotency-Key", "abc")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"seeds\":[\"" + S + "/\"]}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.links.pages").exists())
                .andReturn();
        String id = body(r).get("jobId").asText();
        mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").header("Idempotency-Key", "abc")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"seeds\":[\"" + S + "/\"]}"))
                .andExpect(jsonPath("$.jobId").value(id));
        mvc.perform(get("/v1/crawls/" + id).header("X-Tenant-Id", "t1")).andExpect(status().isOk());
        mvc.perform(get("/v1/crawls/" + id + "/pages").param("type", "image").header("X-Tenant-Id", "t1"))
                .andExpect(status().isOk());
        mvc.perform(get("/v1/crawls/" + id).header("X-Tenant-Id", "someone-else")).andExpect(status().isNotFound());
    }

    @Test void validationErrorsAreProblems() throws Exception {
        mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seeds\":[\"" + S + "/\"],\"maxDepth\":3,\"mode\":\"SYNC\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail", containsString("use mode=ASYNC")));
        mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seeds\":[]}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/v1/crawls").header("X-Tenant-Id", "t1").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"seeds\":[\"ftp://x.com/\"]}"))
                .andExpect(status().isBadRequest());
    }
}
