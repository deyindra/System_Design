package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Reads a sitemap ({@code <urlset><url><loc>}) extended with navigation edges in their own namespace, so the file
 * stays a valid sitemap for every other consumer:
 *
 * <pre>{@code
 * <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:nav="urn:webcrawler:sitemap-nav">
 *   <url nav:root="true">
 *     <loc>https://example.com/</loc>
 *     <nav:link href="/about"/>          <!-- resolved against <loc> -->
 *   </url>
 *   <url><loc>https://example.com/about</loc><nav:link href="/"/></url>
 * </urlset>
 * }</pre>
 *
 * <ul>
 *   <li>Every URL goes through {@link UrlNormalizer}, so the nodes are the same {@link CanonicalUrl}s the rest of
 *       the crawler uses. One that does not normalize (not http/https, malformed) rejects the file.</li>
 *   <li>Roots: the {@code nav:root="true"} pages; with none marked and no edges (a plain sitemap), every page;
 *       with edges but no marks, none (the crawl then starts from {@link NavigationSitemap#defaultRoots()}).</li>
 *   <li>XXE: DOCTYPE declarations are refused outright, so no entity or external DTD is ever resolved.</li>
 * </ul>
 */
public final class NavigationSitemapParser {

    public static final String NAV_NS = "urn:webcrawler:sitemap-nav";

    private final UrlNormalizer normalizer;

    public NavigationSitemapParser(UrlNormalizer normalizer) { this.normalizer = normalizer; }

    public NavigationSitemap parse(String xml) { return parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))); }

    public NavigationSitemap parse(InputStream xml) {
        Element urlset = read(xml).getDocumentElement();
        if (!"urlset".equals(urlset.getLocalName()))
            throw new IllegalArgumentException("invalid sitemap: root element must be <urlset>, got <" + urlset.getLocalName() + ">");
        Graph<CanonicalUrl> graph = Graph.directed();
        Set<CanonicalUrl> marked = new LinkedHashSet<>();
        for (Element url : children(urlset, "url")) {
            Element loc = children(url, "loc").stream().findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("invalid sitemap: <url> without <loc>"));
            CanonicalUrl page = canonical(null, loc.getTextContent());
            graph.addNode(page);
            if ("true".equalsIgnoreCase(url.getAttributeNS(NAV_NS, "root"))) marked.add(page);
            for (Element link : children(url, "link"))
                if (NAV_NS.equals(link.getNamespaceURI())) graph.addEdge(page, canonical(page.uri(), link.getAttribute("href")));
        }
        Set<CanonicalUrl> roots = !marked.isEmpty() ? marked : graph.edgeCount() == 0 ? graph.nodes() : Set.of();
        return new NavigationSitemap(graph, roots);
    }

    private CanonicalUrl canonical(URI base, String raw) {
        return normalizer.normalize(base, raw == null ? null : raw.strip())
                .orElseThrow(() -> new IllegalArgumentException("invalid sitemap: not an http(s) URL: " + raw));
    }

    private static Document read(InputStream xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(null);   // no stderr noise: the SAXException says what is wrong
            return builder.parse(xml);
        } catch (SAXException | IOException e) {
            throw new IllegalArgumentException("invalid sitemap: " + e.getMessage(), e);
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException("XML parser lacks the XXE protections", e);
        }
    }

    private static List<Element> children(Element parent, String localName) {
        List<Element> out = new ArrayList<>();
        for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling())
            if (n instanceof Element e && localName.equals(e.getLocalName())) out.add(e);
        return out;
    }
}
