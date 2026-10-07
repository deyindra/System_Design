package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NavigationSitemapTest {

    private final UrlNormalizer normalizer = new UrlNormalizer(new ParamRules());

    private CanonicalUrl u(String path) { return normalizer.normalize("https://s.com" + path).orElseThrow(); }

    @Test void defaultRootsAreTheSourcesThenOnePagePerUnreachedCycle() {
        Graph<CanonicalUrl> g = Graph.directed();
        g.addEdge(u("/p"), u("/q"));           // a cycle nothing enters, listed first
        g.addEdge(u("/q"), u("/p"));
        g.addEdge(u("/a"), u("/b"));           // a source, and a cycle it reaches
        g.addEdge(u("/b"), u("/c"));
        g.addEdge(u("/c"), u("/b"));
        g.addNode(u("/lone"));                 // a page with no links at all is a source
        g.addEdge(u("/x"), u("/y"));           // a second cycle nothing enters, entered at its first page
        g.addEdge(u("/y"), u("/x"));

        assertThat(new NavigationSitemap(g, Set.of()).defaultRoots())
                .containsExactly(u("/a"), u("/lone"), u("/p"), u("/x"));
    }

    @Test void markedRootsMustBeInTheGraph() {
        Graph<CanonicalUrl> g = Graph.directed();
        g.addEdge(u("/"), u("/a"));
        assertThat(new NavigationSitemap(g, Set.of(u("/a"))).roots()).containsExactly(u("/a"));
        assertThatThrownBy(() -> new NavigationSitemap(g, Set.of(u("/zzz"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("root not in the sitemap");
    }
}
