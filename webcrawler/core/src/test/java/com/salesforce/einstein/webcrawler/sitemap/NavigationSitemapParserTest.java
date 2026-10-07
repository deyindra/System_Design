package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NavigationSitemapParserTest {

    private final UrlNormalizer normalizer = new UrlNormalizer(new ParamRules());
    private final NavigationSitemapParser parser = new NavigationSitemapParser(normalizer);

    private CanonicalUrl u(String raw) { return normalizer.normalize(raw).orElseThrow(); }

    @Test void readsPagesEdgesAndRoots() {
        NavigationSitemap s = parser.parse("""
                <?xml version="1.0" encoding="UTF-8"?>
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:nav="urn:webcrawler:sitemap-nav">
                  <url nav:root="true">
                    <loc>https://Example.com</loc>
                    <lastmod>2026-01-01</lastmod>
                    <nav:link href="/about#team"/>
                    <nav:link href="https://example.com/blog/"/>
                  </url>
                  <url><loc>https://example.com/about</loc><nav:link href="/"/></url>
                </urlset>
                """);

        assertThat(s.graph().nodes()).containsExactly(u("https://example.com/"), u("https://example.com/about"),
                u("https://example.com/blog/"));
        assertThat(s.graph().successors(u("https://example.com/about"))).containsExactly(u("https://example.com/"));   // a cycle
        assertThat(s.roots()).containsExactly(u("https://example.com/"));
    }

    @Test void aPlainSitemapMakesEveryPageARoot() {
        NavigationSitemap s = parser.parse("""
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                  <url><loc>https://a.com/x</loc></url><url><loc>https://a.com/y</loc></url>
                </urlset>""");
        assertThat(s.roots()).isEqualTo(Set.of(u("https://a.com/x"), u("https://a.com/y")));
    }

    @Test void edgesWithoutMarkedRootsLeaveRootsToTheExecutor() {
        NavigationSitemap s = parser.parse("""
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:nav="urn:webcrawler:sitemap-nav">
                  <url><loc>https://a.com/</loc><nav:link href="/b"/></url>
                </urlset>""");
        assertThat(s.roots()).isEmpty();
    }

    @Test void linksOutsideTheNavNamespaceAreNotEdges() {
        NavigationSitemap s = parser.parse("""
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">
                  <url><loc>https://a.com/</loc><xhtml:link rel="alternate" hreflang="de" href="https://a.com/de"/></url>
                </urlset>""");
        assertThat(s.graph().edgeCount()).isZero();
    }

    @Test void refusesDoctypeSoNoEntityIsEverResolved() {
        assertThatThrownBy(() -> parser.parse("""
                <?xml version="1.0"?>
                <!DOCTYPE urlset [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>&xxe;</loc></url></urlset>"""))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DOCTYPE");
    }

    @Test void rejectsNonHttpUrlsAndOtherDocuments() {
        assertThatThrownBy(() -> parser.parse("<urlset><url><loc>mailto:x@a.com</loc></url></urlset>"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("mailto:x@a.com");
        assertThatThrownBy(() -> parser.parse("<sitemapindex/>"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("<urlset>");
        assertThatThrownBy(() -> parser.parse("<urlset><url/></urlset>")).hasMessageContaining("without <loc>");
    }
}
