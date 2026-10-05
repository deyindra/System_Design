package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.fetch.FetchRequest;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.HttpFetcher;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real fetcher against a local server: size cap and an overall deadline that covers the body. */
class HttpFetcherTest {

    private HttpServer server;
    private String base;

    @BeforeEach void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/big", ex -> {
            ex.sendResponseHeaders(200, 100_000);
            try (OutputStream out = ex.getResponseBody()) { out.write(new byte[100_000]); } catch (IOException ignored) { }
        });
        server.createContext("/drip", ex -> {                       // headers at once, then one byte every 200 ms
            ex.sendResponseHeaders(200, 0);
            try (OutputStream out = ex.getResponseBody()) {
                for (int i = 0; i < 50; i++) { out.write('x'); out.flush(); Thread.sleep(200); }
            } catch (IOException | InterruptedException ignored) { }
        });
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach void stop() { server.stop(0); }

    @Test void bodyIsCutAtMaxBytes() {
        FetchResult r = new HttpFetcher(Duration.ofSeconds(2), Duration.ofSeconds(5))
                .fetch(FetchRequest.of(URI.create(base + "/big"), 1000));
        assertEquals(200, r.status());
        assertTrue(r.truncated());
        assertEquals(1000, r.body().length);
    }

    @Test void slowDripBodyHitsTheDeadline() {
        long start = System.nanoTime();
        FetchResult r = new HttpFetcher(Duration.ofSeconds(2), Duration.ofMillis(600))
                .fetch(FetchRequest.of(URI.create(base + "/drip"), 1 << 20));
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, r.status());
        assertTrue(r.error().contains("deadline"), r.error());
        assertTrue(r.isRetryable());
        assertTrue(ms < 3000, "returned after " + ms + " ms, not after the 10 s the server would take");
    }
}
