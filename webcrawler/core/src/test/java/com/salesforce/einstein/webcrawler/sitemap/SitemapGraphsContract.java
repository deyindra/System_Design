package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** What every {@link SitemapGraphs} must do. Each test uses graphs of its own, so a store may be shared. */
public abstract class SitemapGraphsContract {

    /** The store under test; the subclass creates it and closes it. */
    protected abstract SitemapGraphs graphsUnderTest();

    private SitemapGraphs graphs;
    private final UrlNormalizer normalizer = new UrlNormalizer(new ParamRules());

    @BeforeEach void useGraphsUnderTest() { graphs = graphsUnderTest(); }

    private CanonicalUrl u(String raw) { return normalizer.normalize(raw).orElseThrow(); }

    private static String name() { return "g_" + UUID.randomUUID().toString().replace("-", ""); }

    /** {@code / -> /a, /b, other.com/x; /a -> /, /logo.png}. */
    private Graph<CanonicalUrl> site(String host) {
        Graph<CanonicalUrl> g = Graph.directed();
        CanonicalUrl root = u("https://" + host + "/");
        CanonicalUrl a = u("https://" + host + "/a");
        g.addEdge(root, a);
        g.addEdge(root, u("https://" + host + "/b"));
        g.addEdge(root, u("https://other.com/x"));
        g.addEdge(a, root);
        g.addEdge(a, u("https://" + host + "/logo.png"));
        return g;
    }

    @Test void aLoadedGraphReadsBackItsSuccessors() {
        String name = name();
        graphs.load(name, site("site.com"));
        try {
            Map<CanonicalUrl, List<CanonicalUrl>> next = graphs.open(name)
                    .successors(List.of(u("https://site.com/"), u("https://site.com/a"), u("https://site.com/b"),
                            u("https://nowhere.com/")));
            assertThat(next).containsOnlyKeys(u("https://site.com/"), u("https://site.com/a"), u("https://site.com/b"));
            assertThat(next.get(u("https://site.com/"))).containsExactlyInAnyOrder(u("https://site.com/a"),
                    u("https://site.com/b"), u("https://other.com/x"));
            assertThat(next.get(u("https://site.com/a"))).containsExactlyInAnyOrder(u("https://site.com/"),
                    u("https://site.com/logo.png"));
            assertThat(next.get(u("https://site.com/b"))).isEmpty();
        } finally {
            graphs.drop(name);
        }
    }

    @Test void existsUntilDropped() {
        String name = name();
        assertThat(graphs.exists(name)).isFalse();
        graphs.load(name, site("site.com"));
        assertThat(graphs.exists(name)).isTrue();
        graphs.drop(name);
        assertThat(graphs.exists(name)).isFalse();
        graphs.drop(name);                                         // dropping again is harmless
    }

    @Test void openIsCachedPerName() {
        String name = name();
        graphs.load(name, site("site.com"));
        try {
            GraphStore<CanonicalUrl> store = graphs.open(name);
            assertThat(graphs.open(name)).isSameAs(store);
        } finally {
            graphs.drop(name);
        }
    }

    @Test void graphsAreIsolated() {
        String one = name(), two = name();
        graphs.load(one, site("one.com"));
        graphs.load(two, site("two.com"));
        try {
            assertThat(graphs.open(one).successors(List.of(u("https://two.com/")))).isEmpty();
            assertThat(graphs.open(two).successors(List.of(u("https://two.com/")))).hasSize(1);
            graphs.drop(one);
            assertThat(graphs.open(two).successors(List.of(u("https://two.com/a")))).hasSize(1);
        } finally {
            graphs.drop(one);
            graphs.drop(two);
        }
    }

    @Test void pagesAreShardedByHost() {
        String name = name();
        graphs.load(name, site("site.com"));
        try {
            GraphStore<CanonicalUrl> store = graphs.open(name);
            assertThat(store.shardOf(u("https://site.com/a"))).isEqualTo(store.shardOf(u("https://site.com/b")));
            int shard = store.shardOf(u("https://site.com/"));
            assertThat(store.nodes(shard, null, 100)).contains(u("https://site.com/"), u("https://site.com/a"),
                    u("https://site.com/b"), u("https://site.com/logo.png"));
        } finally {
            graphs.drop(name);
        }
    }

    // ---------------------------------------------------------------- the catalog

    private static final Instant T0 = Instant.parse("2026-01-02T03:04:05.678Z");

    private static SitemapGraphRecord loading(String name, String loadId) {
        return SitemapGraphRecord.loading(name, loadId, "t1", "https://site.com/nav.xml", T0);
    }

    @Test void anEntryIsRegisteredOnce() {
        String name = name();
        try {
            assertThat(graphs.register(loading(name, "l1"))).isTrue();
            assertThat(graphs.register(loading(name, "l2"))).isFalse();
            assertThat(graphs.record(name)).map(SitemapGraphRecord::loadId).contains("l1");
        } finally {
            graphs.unregister(name);
        }
    }

    @Test void anEntryReadsBackAsWritten() {
        String name = name();
        SitemapGraphRecord entry = loading(name, "l1");
        SitemapGraphRecord ready = entry.ready(7, 9, List.of("https://site.com/", "https://site.com/a"),
                T0.plusSeconds(5));
        SitemapGraphRecord failed = entry.failed("sitemap has no pages", T0.plusSeconds(6));
        try {
            graphs.register(entry);
            assertThat(graphs.record(name)).contains(entry);
            assertThat(graphs.update(ready)).isTrue();
            assertThat(graphs.record(name)).contains(ready);
            assertThat(graphs.update(failed)).isTrue();
            assertThat(graphs.record(name)).contains(failed);
        } finally {
            graphs.unregister(name);
        }
    }

    @Test void onlyTheSameLoadUpdatesAnEntry() {
        String name = name();
        try {
            assertThat(graphs.update(loading(name, "l1").failed("x", T0))).as("no entry").isFalse();
            graphs.register(loading(name, "l2"));
            assertThat(graphs.update(loading(name, "l1").failed("x", T0))).as("another load").isFalse();
            assertThat(graphs.record(name)).map(SitemapGraphRecord::status).contains(SitemapGraphRecord.Status.LOADING);
        } finally {
            graphs.unregister(name);
        }
    }

    @Test void theCatalogAndTheGraphsAreApart() {
        String name = name();
        graphs.register(loading(name, "l1"));
        graphs.load(name, site("site.com"));
        graphs.drop(name);
        assertThat(graphs.record(name)).as("drop keeps the entry").isPresent();
        graphs.load(name, site("site.com"));
        graphs.unregister(name);
        assertThat(graphs.record(name)).isEmpty();
        assertThat(graphs.exists(name)).as("unregister keeps the graph").isTrue();
        graphs.drop(name);
        graphs.unregister(name);                                   // unregistering again is harmless
        assertThat(graphs.record(name)).isEmpty();
    }
}
