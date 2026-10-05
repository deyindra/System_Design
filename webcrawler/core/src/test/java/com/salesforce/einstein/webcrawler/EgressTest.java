package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.fetch.CachingDnsResolver;
import com.salesforce.einstein.webcrawler.fetch.EgressFilteringFetcher;
import com.salesforce.einstein.webcrawler.fetch.EgressPolicy;
import com.salesforce.einstein.webcrawler.fetch.FetchRequest;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.Scope;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SSRF: tenant-supplied URLs must not reach loopback, metadata or internal addresses. */
@SuppressWarnings("HttpUrlsUsage")   // plain-http internal targets are exactly what an attacker would submit
class EgressTest {

    /** Fake DNS: host → address literals (parsed locally, no lookup). */
    private static final Map<String, String[]> DNS = Map.of(
            "public.com", new String[]{"93.184.216.34"},
            "v6.com", new String[]{"2606:4700::1111"},
            "localhost", new String[]{"127.0.0.1"},
            "metadata.internal", new String[]{"169.254.169.254"},
            "corp.internal", new String[]{"10.1.2.3"},
            "cgnat.example", new String[]{"100.64.1.1"},
            "ec2-v6-metadata", new String[]{"fd00:ec2::254"},
            "nat64-metadata", new String[]{"64:ff9b::a9fe:a9fe"},
            "split.example", new String[]{"93.184.216.34", "192.168.0.10"});   // one bad answer is enough

    private static EgressPolicy policy(boolean allowPrivate) {
        CachingDnsResolver dns = new CachingDnsResolver(h -> {
            String[] ips = h.matches("[0-9.]+|\\[[0-9a-f:]+]") ? new String[]{h} : DNS.get(h);   // literals: no lookup
            if (ips == null) throw new IllegalStateException("NXDOMAIN " + h);
            InetAddress[] out = new InetAddress[ips.length];
            try { for (int i = 0; i < ips.length; i++) out[i] = InetAddress.getByName(ips[i]); }
            catch (UnknownHostException e) { throw new IllegalStateException(e); }
            return out;
        }, Clock.systemUTC(), Duration.ofMinutes(1), Duration.ofMinutes(1));
        return new EgressPolicy(dns, allowPrivate, Set.of(80, 443, 8080, 8443));
    }

    private static boolean allowed(EgressPolicy p, String url) throws UnknownHostException {
        return p.check(URI.create(url)).isEmpty();
    }

    @Test void publicAddressesOnWebPortsAreAllowed() throws Exception {
        EgressPolicy p = policy(false);
        assertTrue(allowed(p, "https://public.com/"));
        assertTrue(allowed(p, "http://public.com:8080/x"));
        assertTrue(allowed(p, "https://v6.com/"));
    }

    @Test void internalAddressesAreRefused() throws Exception {
        EgressPolicy p = policy(false);
        for (String host : new String[]{"localhost", "metadata.internal", "corp.internal", "cgnat.example",
                "ec2-v6-metadata", "nat64-metadata", "split.example", "127.0.0.1", "[::1]", "0.0.0.0", "169.254.169.254"})
            assertFalse(allowed(p, "http://" + host + "/"), host);
    }

    @Test void schemesAndPortsAreRestricted() throws Exception {
        EgressPolicy p = policy(false);
        assertFalse(allowed(p, "https://public.com:22/"));
        assertFalse(allowed(p, "https://public.com:6379/"));
        assertFalse(allowed(p, "ftp://public.com/"));
        assertTrue(allowed(policy(true), "http://localhost:8080/"), "allow-private is for local development");
    }

    @Test void filteringFetcherNeverSendsARefusedRequestAndDoesNotRetryIt() {
        FakeWeb web = new FakeWeb().page("http://localhost/", "<p>admin</p>");
        EgressFilteringFetcher f = new EgressFilteringFetcher(web, policy(false));
        FetchResult r = f.fetch(FetchRequest.of(URI.create("http://localhost/"), 1024));
        assertEquals(FetchResult.BLOCKED, r.status());
        assertFalse(r.isRetryable());
        assertEquals(0, web.hits("http://localhost/"));
    }

    @Test void aPublicPageCannotLeadTheCrawlerIntoTheInternalNetwork() throws Exception {
        FakeWeb web = new FakeWeb()
                .page("https://public.com/", "<a href='http://metadata.internal/latest/meta-data/'>x</a>")
                .redirect("https://public.com/go", "http://corp.internal/admin")
                .page("http://metadata.internal/latest/meta-data/", "secret")
                .page("http://corp.internal/admin", "secret");
        try (CrawlEngine engine = TestEngines.engine(new EgressFilteringFetcher(web, policy(false)), 2)) {
            CrawlJob job = engine.submit(CrawlRequest.builder("t1", "https://public.com/").scope(Scope.ANY)
                    .respectRobots(false).build(), null);
            engine.submit(CrawlRequest.builder("t1", "https://public.com/go").scope(Scope.ANY)
                    .respectRobots(false).build(), null);
            engine.await(job.jobId(), Duration.ofSeconds(5));
            Thread.sleep(200);
            assertEquals(0, web.hits("http://metadata.internal/latest/meta-data/"));
            assertEquals(0, web.hits("http://corp.internal/admin"));
            JobPage meta = engine.jobs().page(job.jobId(),
                    engine.normalizer().normalize("http://metadata.internal/latest/meta-data/").orElseThrow().hash()).orElseThrow();
            assertEquals(PageStatus.FAILED, meta.status());
            assertTrue(meta.error().contains("egress"));
        }
    }
}
