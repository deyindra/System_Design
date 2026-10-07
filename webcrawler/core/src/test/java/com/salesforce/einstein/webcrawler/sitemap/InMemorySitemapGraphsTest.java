package com.salesforce.einstein.webcrawler.sitemap;

class InMemorySitemapGraphsTest extends SitemapGraphsContract {

    private final SitemapGraphs graphs = new InMemorySitemapGraphs(4);

    @Override protected SitemapGraphs graphsUnderTest() { return graphs; }
}
