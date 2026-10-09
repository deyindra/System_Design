package com.salesforce.einstein.hierarchy.service;

import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.domain.ConflictException;
import com.salesforce.einstein.hierarchy.domain.CreateNode;
import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.domain.Cursors;
import com.salesforce.einstein.hierarchy.domain.IdempotencyConflictException;
import com.salesforce.einstein.hierarchy.domain.InvalidRequestException;
import com.salesforce.einstein.hierarchy.domain.LexoRank;
import com.salesforce.einstein.hierarchy.domain.MoveJob;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.MoveOutcome;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.NodeDetail;
import com.salesforce.einstein.hierarchy.domain.NodeStatus;
import com.salesforce.einstein.hierarchy.domain.NotFoundException;
import com.salesforce.einstein.hierarchy.domain.Overlay;
import com.salesforce.einstein.hierarchy.domain.Page;
import com.salesforce.einstein.hierarchy.domain.Paths;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.StaleVersionException;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.domain.TypeRules;
import com.salesforce.einstein.hierarchy.domain.Written;
import com.salesforce.einstein.hierarchy.spi.IdGenerator;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import com.salesforce.einstein.hierarchy.store.Db;
import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.store.SpaceLocks;
import com.salesforce.einstein.hierarchy.store.TreeStore;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The tree operations (DESIGN.md §7 and §8).
 *
 * <ul>
 *   <li><b>Reads</b> run in one REPEATABLE READ snapshot on a replica the read token allows, and always see effective
 *       paths: a RUNNING large move's overlay is applied.</li>
 *   <li><b>Create</b> holds the space's shared lock, so it can't interleave with a move rewriting its parent's path.</li>
 *   <li><b>Move</b> holds the exclusive lock of both spaces, so moves serialize: the second of two opposing moves sees
 *       the first one's result and fails the cycle check.</li>
 *   <li><b>Rename, edit, reorder, trash</b> touch one row, guarded by OCC on {@code version}; no tree lock.</li>
 *   <li>Every write appends its event to the outbox in the same transaction and returns a read token.</li>
 * </ul>
 */
public final class TreeService {
    private static final Logger log = LoggerFactory.getLogger(TreeService.class);
    public static final int DEFAULT_PAGE = 50;
    public static final int MAX_PAGE = 1000;
    private static final int MAX_TITLE = 255;
    private static final Pattern SPACE_KEY = Pattern.compile("[A-Z][A-Z0-9]{1,15}");
    private static final Pattern PRINCIPAL = Pattern.compile("(user|group):[A-Za-z0-9._@:-]{1,64}");

    /**
     * @param syncMoveLimit  subtrees up to this size move in one statement; larger ones get a background job
     * @param lockTimeout    longest wait for a space lock before answering 409 Retry-After
     * @param purgeBatchSize rows deleted per purge transaction
     */
    public record Settings(int syncMoveLimit, Duration lockTimeout, int purgeBatchSize) {
    }

    private final ShardRouter shards;
    private final TreeStore store;
    private final IdGenerator ids;
    private final TreeCache cache;
    private final Settings settings;

    public TreeService(ShardRouter shards, TreeStore store, IdGenerator ids, TreeCache cache, Settings settings) {
        this.shards = shards;
        this.store = store;
        this.ids = ids;
        this.cache = cache;
        this.settings = settings;
    }

    // =================================================================== spaces

    public Written<Space> createSpace(Caller c, String key, String title) {
        if (!SPACE_KEY.matcher(key).matches()) {
            throw new InvalidRequestException("space key must match " + SPACE_KEY.pattern());
        }
        String t = requireTitle(title);
        Shard shard = shards.shardFor(c.tenantId());
        Space space = new Space(ids.nextId(), key, ids.nextId(), 0);
        try {
            shard.primary().write(j -> {
                store.insertSpace(j, c.tenantId(), space);
                Instant now = Instant.now();
                store.insertNode(j, c.tenantId(), new Node(space.rootNodeId(), space.id(), null,
                        Paths.root(space.rootNodeId()), 0, LexoRank.between(null, null), TypeRules.SPACE, t,
                        NodeStatus.ACTIVE, 1, c.actorId(), now, now));
                store.appendEvent(j, TreeEvent.of(c.tenantId(), TreeEvent.Type.SPACE_CREATED, space.id(),
                        space.rootNodeId(), null));
                return space;
            });
        } catch (DuplicateKeyException e) {
            throw ConflictException.permanent("space key '" + key + "' is already taken");
        }
        return new Written<>(space, shard.tokenNow());
    }

    public Space space(Caller c, long spaceId, ReadToken token) {
        return read(c, token, j -> requireSpace(j, c.tenantId(), spaceId));
    }

    /** The trash view: TRASHED roots of the space the caller may see, newest first. */
    public List<Node> trash(Caller c, long spaceId, ReadToken token) {
        return read(c, token, j -> {
            requireSpace(j, c.tenantId(), spaceId);
            Overlay o = store.overlay(j, c.tenantId(), spaceId);
            List<Node> nodes = store.trashedNodes(j, c.tenantId(), spaceId, MAX_PAGE).stream().map(o::apply).toList();
            return AccessControl.visible(store, j, c, nodes, List.of());
        });
    }

    // =================================================================== reads

    public NodeDetail get(Caller c, long id, boolean withBody, ReadToken token) {
        return read(c, token, j -> {
            Node n = locate(j, c.tenantId(), id).node();
            AccessControl.requireView(store, j, c, n);
            boolean inTrash = store.anyTrashed(j, c.tenantId(), Paths.ids(n.path()));
            return new NodeDetail(n, inTrash, withBody ? store.body(j, c.tenantId(), id) : null);
        });
    }

    /** Breadcrumbs, root first, excluding the node itself. */
    public List<Crumb> ancestors(Caller c, long id, ReadToken token) {
        List<Crumb> path = path(c, id, token);
        return path.subList(0, path.size() - 1);
    }

    /** One keyset page of children in sibling order. */
    public Page<Node> children(Caller c, long id, int limit, @Nullable String cursor, ReadToken token) {
        int lim = pageSize(limit);
        Cursors.ChildKey after = Cursors.child(cursor);
        UUID t = c.tenantId();
        boolean cacheable = token.isNone() && after == null && lim == DEFAULT_PAGE;
        if (cacheable) {
            Optional<CachedPath> cp = cachedPath(t, id);
            Optional<TreeCache.ChildrenPage> page = cp.flatMap(p -> cache.children(t, id)
                    .filter(ch -> ch.treeVersion() == p.treeVersion()));
            if (cp.isPresent() && page.isPresent()) {
                if (cp.get().crumbs().stream().anyMatch(cr -> cr.status() == NodeStatus.TRASHED)) {
                    throw new NotFoundException("node " + id + " is in the trash");
                }
                return read(c, token, j -> {
                    checkViewByIds(j, c, id, cp.get().ids());
                    List<Node> items = page.get().items();
                    return new Page<>(AccessControl.visible(store, j, c, items, cp.get().ids()),
                            page.get().hasMore() ? Cursors.ofChild(last(items).rank(), last(items).id()) : null);
                });
            }
        }
        record ChildRows(List<Node> rows, List<Node> visible) {
        }
        Snapshot<ChildRows> snap = read(c, token, j -> {
            Located parent = locate(j, t, id);
            AccessControl.requireView(store, j, c, parent.node());
            requireNotInTrash(j, t, parent.node());
            List<Node> rows = store.children(j, t, id, after, lim + 1).stream().map(parent.overlay()::apply).toList();
            List<Node> pageRows = rows.size() > lim ? rows.subList(0, lim) : rows;
            List<Node> visible = AccessControl.visible(store, j, c, pageRows, Paths.ids(parent.node().path()));
            return new Snapshot<>(new ChildRows(rows, visible), parent.node(),
                    store.treeVersion(j, t, parent.node().spaceId()));
        });
        List<Node> rows = snap.value().rows();
        boolean hasMore = rows.size() > lim;
        List<Node> pageRows = hasMore ? rows.subList(0, lim) : rows;
        if (cacheable) {
            // The unfiltered page is cached; each reader is ACL-filtered against the database.
            remember(t, snap.node(), snap.treeVersion());
            cache.putChildren(t, id, new TreeCache.ChildrenPage(snap.treeVersion(), pageRows, hasMore));
        }
        return new Page<>(snap.value().visible(),
                hasMore ? Cursors.ofChild(last(pageRows).rank(), last(pageRows).id()) : null);
    }

    /** A subtree page in pre-order (depth first), at most {@code depth} levels below the node. */
    public Page<Node> descendants(Caller c, long id, int depth, int limit, @Nullable String cursor, ReadToken token) {
        if (depth < 1) {
            throw new InvalidRequestException("depth must be at least 1");
        }
        int lim = pageSize(limit);
        String afterPath = Cursors.path(cursor);
        UUID t = c.tenantId();
        return read(c, token, j -> {
            Located root = locate(j, t, id);
            Node r = root.node();
            AccessControl.requireView(store, j, c, r);
            requireNotInTrash(j, t, r);
            String after = afterPath == null ? r.path() : afterPath;
            if (!after.startsWith(r.path())) {
                throw new InvalidRequestException("cursor does not belong to this subtree");
            }
            int maxDepth = (int) Math.min((long) r.depth() + depth, Paths.MAX_DEPTH);
            List<Long> trashed = store.trashedIds(j, t, r.spaceId());
            List<Node> rows = store.descendants(j, t, r.path(), root.overlay(), after, maxDepth, trashed, lim + 1);
            boolean hasMore = rows.size() > lim;
            List<Node> pageRows = hasMore ? rows.subList(0, lim) : rows;
            return new Page<>(AccessControl.visible(store, j, c, pageRows, Paths.ids(r.path())),
                    hasMore ? Cursors.ofPath(last(pageRows).path()) : null);
        });
    }

    public MoveJob moveJob(Caller c, long jobId, ReadToken token) {
        return read(c, token, j -> store.moveJob(j, c.tenantId(), jobId)
                .orElseThrow(() -> new NotFoundException("move job " + jobId + " not found")));
    }

    public Map<RestrictionOp, List<String>> restrictions(Caller c, long id, ReadToken token) {
        return read(c, token, j -> {
            Node n = locate(j, c.tenantId(), id).node();
            AccessControl.requireView(store, j, c, n);
            Map<RestrictionOp, List<String>> out = new TreeMap<>();
            store.restrictions(j, c.tenantId(), List.of(id)).getOrDefault(id, Map.of())
                    .forEach((op, ps) -> out.put(op, List.copyOf(ps)));
            return out;
        });
    }

    // =================================================================== create

    /** Creates a child. With an idempotency key, a retry of the same request returns the node created the first time. */
    public Written<Node> create(Caller c, long spaceId, CreateNode req, @Nullable String idempotencyKey) {
        String title = requireTitle(req.title());
        if (TypeRules.isUnknown(req.type())) {
            throw new InvalidRequestException("unknown node type '" + req.type() + "'");
        }
        UUID t = c.tenantId();
        Shard shard = shards.shardFor(t);
        long id = ids.nextId();
        String hash = requestHash(spaceId, req);
        Node created = shard.primary().write(j -> {
            if (idempotencyKey != null && !store.claimIdempotencyKey(j, t, idempotencyKey, hash, id)) {
                return replay(j, c, idempotencyKey, hash);
            }
            SpaceLocks.shared(j, t, spaceId, settings.lockTimeout());
            Node parent = locate(j, t, req.parentId()).node();
            if (parent.spaceId() != spaceId) {
                throw new InvalidRequestException("parent " + parent.id() + " is not in space " + spaceId);
            }
            AccessControl.requireEdit(store, j, c, parent);
            if (parent.depth() >= Paths.MAX_DEPTH) {
                throw ConflictException.permanent("the tree is at most " + (Paths.MAX_DEPTH + 1) + " levels deep");
            }
            if (store.anyTrashed(j, t, Paths.ids(parent.path()))) {
                throw ConflictException.permanent("parent " + parent.id() + " is in the trash");
            }
            TypeRules.checkChild(parent.type(), req.type());
            String rank = rankFor(j, t, parent.id(), req.afterId(), req.beforeId(), id);
            // The parent's *effective* path: during a large move the new row is born at its final path.
            Instant now = Instant.now();
            Node n = new Node(id, spaceId, parent.id(), Paths.child(parent.path(), id), parent.depth() + 1, rank,
                    req.type(), title, NodeStatus.ACTIVE, 1, c.actorId(), now, now);
            store.insertNode(j, t, n);
            if (req.body() != null) {
                store.upsertBody(j, t, id, req.body());
            }
            store.appendEvent(j, TreeEvent.of(t, TreeEvent.Type.NODE_CREATED, spaceId, id, parent.id()));
            return locate(j, t, id).node();
        });
        if (created.id() == id) {
            evict(t, id, created.parentId());
        }
        return new Written<>(created, shard.tokenNow());
    }

    private Node replay(JdbcTemplate j, Caller c, String key, String hash) {
        TreeStore.IdempotencyRecord rec = store.idempotencyKey(j, c.tenantId(), key)
                .orElseThrow(() -> new IllegalStateException("idempotency key vanished: " + key));
        if (!rec.requestHash().equals(hash)) {
            throw new IdempotencyConflictException(key);
        }
        Node n = locate(j, c.tenantId(), rec.nodeId()).node();
        AccessControl.requireView(store, j, c, n);
        return n;
    }

    // =================================================================== single-row edits (OCC, no tree lock)

    public Written<Node> update(Caller c, long id, long expectedVersion, @Nullable String title, @Nullable String body) {
        if (title == null && body == null) {
            throw new InvalidRequestException("nothing to change: give a title or a body");
        }
        String newTitle = title == null ? null : requireTitle(title);
        UUID t = c.tenantId();
        Shard shard = shards.shardFor(t);
        Node n = shard.primary().write(j -> {
            Node cur = locate(j, t, id).node();
            AccessControl.requireEdit(store, j, c, cur);
            if (store.anyTrashed(j, t, Paths.ids(cur.path()))) {
                throw ConflictException.permanent("node " + id + " is in the trash; restore it first");
            }
            if (store.updateTitle(j, t, id, expectedVersion, newTitle) == 0) {
                throw new StaleVersionException(id, expectedVersion);
            }
            if (body != null) {
                store.upsertBody(j, t, id, body);
            }
            store.appendEvent(j, TreeEvent.of(t, TreeEvent.Type.NODE_UPDATED, cur.spaceId(), id, cur.parentId()));
            return locate(j, t, id).node();
        });
        evict(t, id, n.parentId());
        return new Written<>(n, shard.tokenNow());
    }

    public Written<Node> trash(Caller c, long id, long expectedVersion) {
        return setStatus(c, id, expectedVersion, NodeStatus.TRASHED);
    }

    public Written<Node> restore(Caller c, long id, long expectedVersion) {
        return setStatus(c, id, expectedVersion, NodeStatus.ACTIVE);
    }

    /** O(1): only the subtree's root changes; reads hide everything below a TRASHED id on the path. */
    private Written<Node> setStatus(Caller c, long id, long expectedVersion, NodeStatus target) {
        UUID t = c.tenantId();
        Shard shard = shards.shardFor(t);
        Node n = shard.primary().write(j -> {
            Node cur = locate(j, t, id).node();
            if (cur.isRoot()) {
                throw new InvalidRequestException("a space root cannot be trashed");
            }
            AccessControl.requireEdit(store, j, c, cur);
            if (cur.status() == target) {
                throw ConflictException.permanent("node " + id + " is already " + target);
            }
            if (store.setStatus(j, t, id, expectedVersion, target) == 0) {
                throw new StaleVersionException(id, expectedVersion);
            }
            TreeEvent.Type type = target == NodeStatus.TRASHED ? TreeEvent.Type.NODE_TRASHED : TreeEvent.Type.NODE_RESTORED;
            store.appendEvent(j, TreeEvent.of(t, type, cur.spaceId(), id, cur.parentId()));
            return locate(j, t, id).node();
        });
        evict(t, id, n.parentId());
        return new Written<>(n, shard.tokenNow());
    }

    public Written<Map<RestrictionOp, List<String>>> setRestrictions(Caller c, long id, RestrictionOp op,
                                                                     Collection<String> principals) {
        if (principals.size() > 100) {
            throw new InvalidRequestException("at most 100 principals per restriction");
        }
        for (String p : principals) {
            if (!PRINCIPAL.matcher(p).matches()) {
                throw new InvalidRequestException("principal '" + p + "' must match " + PRINCIPAL.pattern());
            }
        }
        UUID t = c.tenantId();
        Shard shard = shards.shardFor(t);
        shard.primary().write(j -> {
            Node cur = locate(j, t, id).node();
            AccessControl.requireEdit(store, j, c, cur);
            store.replaceRestrictions(j, t, id, op, principals);
            store.appendEvent(j, TreeEvent.of(t, TreeEvent.Type.RESTRICTIONS_CHANGED, cur.spaceId(), id,
                    cur.parentId()));
            return id;
        });
        ReadToken token = shard.tokenNow();
        return new Written<>(restrictions(c, id, token), token);
    }

    // =================================================================== move

    /**
     * Moves (or, to the same parent, reorders) a node with its subtree.
     *
     * @param expectedVersion the node's version from {@code If-Match}, or null to move whatever is current
     */
    public Written<MoveOutcome> move(Caller c, long id, MoveNode req, @Nullable Long expectedVersion) {
        if (req.newParentId() == id) {
            throw ConflictException.permanent("cannot move a node under itself");
        }
        UUID t = c.tenantId();
        Shard shard = shards.shardFor(t);
        Db db = shard.primary();
        Node before = db.read(j -> store.node(j, t, id).orElseThrow(() -> notFound(id)));
        Node newParent = db.read(j -> store.node(j, t, req.newParentId()).orElseThrow(() -> notFound(req.newParentId())));
        if (Objects.equals(before.parentId(), newParent.id())) {
            return reorder(c, shard, id, req, expectedVersion);
        }
        MoveOutcome outcome = db.write(j -> moveLocked(j, c, id, req, expectedVersion,
                List.of(before.spaceId(), newParent.spaceId())));
        Node moved = outcome.node();
        cache.raiseTreeVersion(t, moved.spaceId(), db.read(j -> store.treeVersion(j, t, moved.spaceId())));
        if (before.spaceId() != moved.spaceId()) {
            cache.raiseTreeVersion(t, before.spaceId(), db.read(j -> store.treeVersion(j, t, before.spaceId())));
        }
        evict(t, id, before.parentId(), moved.parentId());
        return new Written<>(outcome, shard.tokenNow());
    }

    private MoveOutcome moveLocked(JdbcTemplate j, Caller c, long id, MoveNode req, @Nullable Long expectedVersion,
                                   List<Long> lockedSpaces) {
        UUID t = c.tenantId();
        SpaceLocks.exclusive(j, t, lockedSpaces, settings.lockTimeout());
        // Re-read under the lock: what was read before locking may be stale.
        Node node = store.node(j, t, id).orElseThrow(() -> notFound(id));
        Node parent = store.node(j, t, req.newParentId()).orElseThrow(() -> notFound(req.newParentId()));
        if (!lockedSpaces.contains(node.spaceId()) || !lockedSpaces.contains(parent.spaceId())) {
            throw ConflictException.retryLater("the node was moved to another space concurrently; retry");
        }
        if (store.runningJob(j, t, node.spaceId()).isPresent() || store.runningJob(j, t, parent.spaceId()).isPresent()) {
            throw ConflictException.retryLater("a large move is still running in this space; retry when it finishes");
        }
        // No overlay is active in either space, so stored paths are effective paths from here on.
        if (node.isRoot()) {
            throw new InvalidRequestException("a space root cannot be moved");
        }
        if (expectedVersion != null && node.version() != expectedVersion) {
            throw new StaleVersionException(id, expectedVersion);
        }
        AccessControl.requireEdit(store, j, c, node);
        AccessControl.requireEdit(store, j, c, parent);
        if (store.anyTrashed(j, t, Paths.ids(node.path()))) {
            throw ConflictException.permanent("node " + id + " is in the trash; restore it first");
        }
        if (store.anyTrashed(j, t, Paths.ids(parent.path()))) {
            throw ConflictException.permanent("target " + parent.id() + " is in the trash");
        }
        TypeRules.checkChild(parent.type(), node.type());
        String oldPrefix = node.path();
        if (parent.path().startsWith(oldPrefix)) {
            throw ConflictException.permanent("cycle: target " + parent.id() + " is inside the moved subtree");
        }
        String newPrefix = Paths.child(parent.path(), id);
        int depthDelta = parent.depth() + 1 - node.depth();
        String rank = rankFor(j, t, parent.id(), req.afterId(), req.beforeId(), id);
        int size = store.countUnder(j, t, oldPrefix, settings.syncMoveLimit() + 1);
        if (store.maxDepthUnder(j, t, oldPrefix) + depthDelta > Paths.MAX_DEPTH) {
            throw ConflictException.permanent("the move would make the tree deeper than " + (Paths.MAX_DEPTH + 1)
                    + " levels");
        }
        boolean crossSpace = node.spaceId() != parent.spaceId();
        if (size <= settings.syncMoveLimit()) {
            store.rewritePrefix(j, t, oldPrefix, newPrefix, depthDelta, parent.spaceId());
            store.reparent(j, t, id, parent.id(), rank);
            long tv = store.bumpTreeVersion(j, t, parent.spaceId());
            long oldTv = crossSpace ? store.bumpTreeVersion(j, t, node.spaceId()) : 0;
            store.appendEvent(j, TreeEvent.moved(t, TreeEvent.Type.NODE_MOVED, parent.spaceId(), id, parent.id(),
                    node.parentId(), crossSpace ? node.spaceId() : null, oldTv, null, tv));
            return new MoveOutcome.Done(store.node(j, t, id).orElseThrow());
        }
        if (crossSpace) {
            throw ConflictException.permanent("subtree of " + id + " has more than " + settings.syncMoveLimit()
                    + " nodes, too large to move across spaces");
        }
        store.moveRootOnly(j, t, id, parent.id(), newPrefix, parent.depth() + 1, rank);
        MoveJob job = new MoveJob(ids.nextId(), node.spaceId(), id, oldPrefix, newPrefix, depthDelta,
                MoveJob.State.RUNNING, 0, Instant.now(), null);
        store.insertMoveJob(j, t, job);
        long tv = store.bumpTreeVersion(j, t, node.spaceId());
        store.appendEvent(j, TreeEvent.moved(t, TreeEvent.Type.MOVE_STARTED, node.spaceId(), id, parent.id(),
                node.parentId(), null, 0, job.id(), tv));
        log.info("large move of {} ({}+ nodes) accepted as job {}", id, size, job.id());
        return new MoveOutcome.Accepted(store.node(j, t, id).orElseThrow(),
                store.moveJob(j, t, job.id()).orElseThrow());
    }

    /** Same parent: only the rank changes, guarded by OCC on the node. No tree lock. */
    private Written<MoveOutcome> reorder(Caller c, Shard shard, long id, MoveNode req, @Nullable Long expectedVersion) {
        UUID t = c.tenantId();
        Node n = shard.primary().write(j -> {
            Node cur = locate(j, t, id).node();
            if (!Objects.equals(cur.parentId(), req.newParentId())) {
                throw ConflictException.retryLater("node " + id + " was moved concurrently; retry");
            }
            long version = expectedVersion != null ? expectedVersion : cur.version();
            AccessControl.requireEdit(store, j, c, cur);
            String rank = rankFor(j, t, req.newParentId(), req.afterId(), req.beforeId(), id);
            if (store.setRank(j, t, id, version, rank) == 0) {
                throw new StaleVersionException(id, version);
            }
            store.appendEvent(j, TreeEvent.of(t, TreeEvent.Type.NODE_REORDERED, cur.spaceId(), id, cur.parentId()));
            return locate(j, t, id).node();
        });
        evict(t, id, n.parentId());
        return new Written<>(new MoveOutcome.Done(n), shard.tokenNow());
    }

    // =================================================================== purge

    /**
     * Permanently deletes a trashed subtree in batches, deepest first. Each batch holds the space's exclusive lock, so
     * no create can add a child to a row being deleted, and re-checks that the root is still trashed.
     *
     * @return the number of nodes deleted
     */
    public Written<Long> purge(Caller c, long id, long expectedVersion) {
        UUID t = c.tenantId();
        Shard shard = shards.shardFor(t);
        Db db = shard.primary();
        Node root = db.write(j -> {
            Node cur = locate(j, t, id).node();
            AccessControl.requireEdit(store, j, c, cur);
            if (cur.status() != NodeStatus.TRASHED) {
                throw ConflictException.permanent("only a trashed node can be purged; trash it first");
            }
            if (cur.version() != expectedVersion) {
                throw new StaleVersionException(id, expectedVersion);
            }
            return cur;
        });
        long purged = 0;
        while (true) {
            int n = db.write(j -> {
                SpaceLocks.exclusive(j, t, List.of(root.spaceId()), settings.lockTimeout());
                Optional<Node> cur = store.node(j, t, id);
                if (cur.isEmpty()) {
                    return 0;
                }
                if (cur.get().status() != NodeStatus.TRASHED) {
                    throw ConflictException.permanent("node " + id + " was restored during the purge");
                }
                if (store.runningJob(j, t, cur.get().spaceId()).isPresent()) {
                    throw ConflictException.retryLater("a large move is running in this space; retry the purge");
                }
                int deleted = store.deleteBatch(j, t, cur.get().path(), settings.purgeBatchSize());
                if (store.node(j, t, id).isEmpty()) {
                    store.appendEvent(j, TreeEvent.of(t, TreeEvent.Type.NODE_PURGED, cur.get().spaceId(), id,
                            cur.get().parentId()));
                }
                return deleted;
            });
            purged += n;
            if (n < settings.purgeBatchSize()) {
                break;
            }
        }
        evict(t, id, root.parentId());
        return new Written<>(purged, shard.tokenNow());
    }

    // =================================================================== helpers

    /** A node with the overlay of its space already applied, and that overlay (for its descendants). */
    private record Located(Node node, Overlay overlay) {
    }

    private record Snapshot<T>(T value, Node node, long treeVersion) {
    }

    /** Breadcrumb from the cache: the path's ids and a crumb for each, valid at the space's current tree version. */
    private record CachedPath(long treeVersion, List<Long> ids, List<Crumb> crumbs) {
    }

    private Located locate(JdbcTemplate j, UUID t, long id) {
        Node stored = store.node(j, t, id).orElseThrow(() -> notFound(id));
        Overlay o = store.overlay(j, t, stored.spaceId());
        return new Located(o.apply(stored), o);
    }

    private <T> T read(Caller c, ReadToken token, java.util.function.Function<JdbcTemplate, T> work) {
        return shards.shardFor(c.tenantId()).forRead(token).read(work);
    }

    /** Root first, ending with the node itself; served from the cache when it is current. */
    private List<Crumb> path(Caller c, long id, ReadToken token) {
        UUID t = c.tenantId();
        if (token.isNone()) {
            Optional<CachedPath> hit = cachedPath(t, id);
            if (hit.isPresent()) {
                read(c, token, j -> {
                    checkViewByIds(j, c, id, hit.get().ids());
                    return true;
                });
                return hit.get().crumbs();
            }
        }
        Snapshot<List<Crumb>> snap = read(c, token, j -> {
            Located located = locate(j, t, id);
            Node n = located.node();
            AccessControl.requireView(store, j, c, n);
            List<Long> pathIds = Paths.ids(n.path());
            Map<Long, Node> byId = store.nodes(j, t, pathIds).stream()
                    .collect(Collectors.toMap(Node::id, located.overlay()::apply));
            List<Crumb> crumbs = new ArrayList<>(pathIds.size());
            for (long pid : pathIds) {
                Node a = byId.get(pid);
                if (a == null) {
                    throw new IllegalStateException("path of " + id + " names missing node " + pid);
                }
                crumbs.add(new Crumb(a.id(), a.title(), a.type(), a.status(), a.depth()));
            }
            return new Snapshot<>(crumbs, n, store.treeVersion(j, t, n.spaceId()));
        });
        if (token.isNone()) {
            remember(t, snap.node(), snap.treeVersion());
            cache.putCrumbs(t, snap.value());
        }
        return snap.value();
    }

    private Optional<CachedPath> cachedPath(UUID t, long id) {
        Optional<TreeCache.Breadcrumb> bc = cache.breadcrumb(t, id);
        if (bc.isEmpty() || cache.treeVersion(t, bc.get().spaceId()).orElse(-1) != bc.get().treeVersion()) {
            return Optional.empty();
        }
        List<Long> pathIds = bc.get().ids();
        Map<Long, Crumb> crumbs = cache.crumbs(t, pathIds);
        if (crumbs.size() != pathIds.size()) {
            return Optional.empty();
        }
        return Optional.of(new CachedPath(bc.get().treeVersion(), pathIds,
                pathIds.stream().map(crumbs::get).toList()));
    }

    /** Caches what a snapshot at {@code treeVersion} showed. Never lowers the version, so stale entries stay dead. */
    private void remember(UUID t, Node n, long treeVersion) {
        cache.raiseTreeVersion(t, n.spaceId(), treeVersion);
        cache.putBreadcrumb(t, n.id(), new TreeCache.Breadcrumb(n.spaceId(), treeVersion, Paths.ids(n.path())));
    }

    private void checkViewByIds(JdbcTemplate j, Caller c, long id, List<Long> pathIds) {
        if (!AccessControl.allowed(store.restrictions(j, c.tenantId(), pathIds), pathIds, c.principals(),
                RestrictionOp.VIEW)) {
            throw notFound(id);
        }
    }

    private void requireNotInTrash(JdbcTemplate j, UUID t, Node n) {
        if (store.anyTrashed(j, t, Paths.ids(n.path()))) {
            throw new NotFoundException("node " + n.id() + " is in the trash");
        }
    }

    private Space requireSpace(JdbcTemplate j, UUID t, long spaceId) {
        return store.space(j, t, spaceId).orElseThrow(() -> new NotFoundException("space " + spaceId + " not found"));
    }

    /**
     * A rank between the named siblings: after {@code afterId}, before {@code beforeId}, or appended after the last
     * child. Concurrent appends may pick the same rank; {@code (rank, node_id)} is still a total order, so a tie only
     * means "after all siblings that share that rank".
     */
    private String rankFor(JdbcTemplate j, UUID t, long parentId, @Nullable Long afterId, @Nullable Long beforeId,
                           long self) {
        String lo;
        String hi;
        if (afterId != null && beforeId != null) {
            lo = sibling(j, t, parentId, afterId, self).rank();
            hi = sibling(j, t, parentId, beforeId, self).rank();
            if (lo.compareTo(hi) > 0) {
                throw new InvalidRequestException("afterId must come before beforeId");
            }
            if (lo.equals(hi)) {
                hi = store.rankAbove(j, t, parentId, lo, self).orElse(null);
            }
        } else if (afterId != null) {
            lo = sibling(j, t, parentId, afterId, self).rank();
            hi = store.rankAbove(j, t, parentId, lo, self).orElse(null);
        } else if (beforeId != null) {
            hi = sibling(j, t, parentId, beforeId, self).rank();
            lo = store.rankBelow(j, t, parentId, hi, self).orElse(null);
        } else {
            lo = store.maxRank(j, t, parentId, self).orElse(null);
            hi = null;
        }
        return LexoRank.between(lo, hi);
    }

    private Node sibling(JdbcTemplate j, UUID t, long parentId, long siblingId, long self) {
        Node s = store.node(j, t, siblingId).orElse(null);
        if (s == null || siblingId == self || !Objects.equals(s.parentId(), parentId)) {
            throw new InvalidRequestException("sibling " + siblingId + " is not a child of " + parentId);
        }
        return s;
    }

    /**
     * After commit: drop the node's cached title and the cached first children page of each affected parent.
     * {@code parents} may contain nulls (a root has no parent); they are skipped.
     */
    private void evict(UUID t, long id, Long... parents) {
        Set<Long> ps = new LinkedHashSet<>();
        for (Long p : parents) {
            if (p != null) {
                ps.add(p);
            }
        }
        cache.evictNode(t, id, ps);
    }

    private static String requireTitle(@Nullable String title) {
        if (title == null || title.isBlank()) {
            throw new InvalidRequestException("title must not be blank");
        }
        String s = title.strip();
        if (s.length() > MAX_TITLE) {
            throw new InvalidRequestException("title is longer than " + MAX_TITLE + " characters");
        }
        return s;
    }

    private static int pageSize(int limit) {
        if (limit < 1 || limit > MAX_PAGE) {
            throw new InvalidRequestException("limit must be between 1 and " + MAX_PAGE);
        }
        return limit;
    }

    private static NotFoundException notFound(long id) {
        return new NotFoundException("node " + id + " not found");
    }

    private static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }

    private static String requestHash(long spaceId, CreateNode req) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("space", spaceId);
        fields.put("parent", req.parentId());
        fields.put("title", req.title());
        fields.put("type", req.type());
        fields.put("after", req.afterId());
        fields.put("before", req.beforeId());
        fields.put("body", req.body());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(fields.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
