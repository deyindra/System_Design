package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A site's navigation, given up front: an edge {@code a -> b} means "page b is linked from page a". Edges are
 * reachability, not dependencies, so cycles ({@code home <-> about}) are normal.
 *
 * @param graph directed, unweighted; may also list assets (they are fetched as leaves, never expanded)
 * @param roots where the crawl starts. Empty means {@link #defaultRoots()}
 */
public record NavigationSitemap(Graph<CanonicalUrl> graph, Set<CanonicalUrl> roots) {

    public NavigationSitemap {
        Objects.requireNonNull(graph, "graph");
        if (!graph.isDirected() || graph.isWeighted()) throw new IllegalArgumentException("graph must be directed and unweighted");
        roots = Set.copyOf(roots);
        for (CanonicalUrl root : roots)
            if (!graph.containsNode(root)) throw new IllegalArgumentException("root not in the sitemap: " + root);
    }

    /**
     * Where a crawl starts when no roots are given: the pages nothing links to, then, for every part of the graph
     * they do not reach (a cycle nothing enters), its first page in file order. Every page is reachable from
     * the result.
     */
    public Set<CanonicalUrl> defaultRoots() {
        Set<CanonicalUrl> out = new LinkedHashSet<>();
        Set<CanonicalUrl> reached = new HashSet<>();
        for (CanonicalUrl page : graph.nodes())
            if (graph.inDegree(page) == 0) reach(page, out, reached);
        for (CanonicalUrl page : graph.nodes())
            if (!reached.contains(page)) reach(page, out, reached);
        return out;
    }

    private void reach(CanonicalUrl root, Set<CanonicalUrl> out, Set<CanonicalUrl> reached) {
        out.add(root);
        Deque<CanonicalUrl> queue = new ArrayDeque<>();
        if (reached.add(root)) queue.add(root);
        while (!queue.isEmpty())
            for (CanonicalUrl next : graph.successors(queue.poll()))
                if (reached.add(next)) queue.add(next);
    }
}
