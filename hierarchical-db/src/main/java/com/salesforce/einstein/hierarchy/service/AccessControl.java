package com.salesforce.einstein.hierarchy.service;

import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.domain.ForbiddenException;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.NotFoundException;
import com.salesforce.einstein.hierarchy.domain.Paths;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.store.TreeStore;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Inherited restrictions (DESIGN.md §7.4). A restriction row on a node applies to its whole subtree, and the caller must
 * pass every restricted node on the path: one {@code = ANY(path ids)} probe. Editing also requires passing the view
 * restrictions. Nodes the caller can't view are reported as not found, so their existence doesn't leak.
 */
final class AccessControl {
    private AccessControl() {
    }

    static boolean allowed(Map<Long, Map<RestrictionOp, Set<String>>> rules, Collection<Long> pathIds,
                           Set<String> principals, RestrictionOp op) {
        for (long id : pathIds) {
            Map<RestrictionOp, Set<String>> r = rules.get(id);
            if (r == null) {
                continue;
            }
            if (denies(r.get(RestrictionOp.VIEW), principals)) {
                return false;
            }
            if (op == RestrictionOp.EDIT && denies(r.get(RestrictionOp.EDIT), principals)) {
                return false;
            }
        }
        return true;
    }

    /** A restriction denies when it names principals and the caller is none of them; absent or empty is open. */
    private static boolean denies(Set<String> allowed, Set<String> principals) {
        return allowed != null && !allowed.isEmpty() && Collections.disjoint(allowed, principals);
    }

    static void requireView(TreeStore store, JdbcTemplate j, Caller c, Node n) {
        List<Long> ids = Paths.ids(n.path());
        if (!allowed(store.restrictions(j, c.tenantId(), ids), ids, c.principals(), RestrictionOp.VIEW)) {
            throw new NotFoundException("node " + n.id() + " not found");
        }
    }

    static void requireEdit(TreeStore store, JdbcTemplate j, Caller c, Node n) {
        List<Long> ids = Paths.ids(n.path());
        Map<Long, Map<RestrictionOp, Set<String>>> rules = store.restrictions(j, c.tenantId(), ids);
        if (!allowed(rules, ids, c.principals(), RestrictionOp.VIEW)) {
            throw new NotFoundException("node " + n.id() + " not found");
        }
        if (!allowed(rules, ids, c.principals(), RestrictionOp.EDIT)) {
            throw new ForbiddenException("you may not edit node " + n.id());
        }
    }

    /**
     * Filters a list (children or a subtree page) to what the caller may view. {@code checked} are path ids already
     * verified (the parent's path); only the ids below them are probed, in one query for the whole page.
     */
    static List<Node> visible(TreeStore store, JdbcTemplate j, Caller c, List<Node> nodes, Collection<Long> checked) {
        Set<Long> ids = new LinkedHashSet<>();
        nodes.forEach(n -> ids.addAll(Paths.ids(n.path())));
        ids.removeAll(Set.copyOf(checked));
        Map<Long, Map<RestrictionOp, Set<String>>> rules = store.restrictions(j, c.tenantId(), ids);
        if (rules.isEmpty()) {
            return nodes;
        }
        List<Node> out = new ArrayList<>(nodes.size());
        for (Node n : nodes) {
            if (allowed(rules, Paths.ids(n.path()), c.principals(), RestrictionOp.VIEW)) {
                out.add(n);
            }
        }
        return out;
    }
}
