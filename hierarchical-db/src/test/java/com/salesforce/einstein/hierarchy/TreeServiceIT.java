package com.salesforce.einstein.hierarchy;

import com.salesforce.einstein.hierarchy.domain.ConflictException;
import com.salesforce.einstein.hierarchy.domain.CreateNode;
import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.domain.ForbiddenException;
import com.salesforce.einstein.hierarchy.domain.IdempotencyConflictException;
import com.salesforce.einstein.hierarchy.domain.InvalidRequestException;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.MoveOutcome;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.NodeStatus;
import com.salesforce.einstein.hierarchy.domain.NotFoundException;
import com.salesforce.einstein.hierarchy.domain.Page;
import com.salesforce.einstein.hierarchy.domain.Paths;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import com.salesforce.einstein.hierarchy.domain.RestrictionOp;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.StaleVersionException;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.store.TreeVerifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class TreeServiceIT {
    private static final ReadToken NONE = ReadToken.NONE;

    private Fixture f;
    private Space s;
    private long root;

    @BeforeEach
    void setUp() {
        f = new Fixture(10_000);
        s = f.space("ENG");
        root = s.rootNodeId();
    }

    /** DESIGN.md §4.3: the example rows, then "move 101 (Architecture) under 104 (Runbooks)". */
    @Test
    void designDocExample() {
        Node arch = f.page(s, root, "Architecture");
        Node adr1 = f.page(s, arch.id(), "ADR-001");
        Node adr2 = f.page(s, arch.id(), "ADR-002");
        Node runbooks = f.page(s, root, "Runbooks");
        Node onCall = f.page(s, runbooks.id(), "On-call");

        assertThat(arch.path()).isEqualTo("/" + root + "/" + arch.id() + "/");
        assertThat(adr1.path()).isEqualTo(arch.path() + adr1.id() + "/");
        assertThat(adr1.depth()).isEqualTo(2);
        assertThat(arch.rank()).isLessThan(runbooks.rank());
        assertThat(adr1.rank()).isLessThan(adr2.rank());

        MoveOutcome out = f.tree.move(f.alice, arch.id(), new MoveNode(runbooks.id(), null, null), null).value();

        assertThat(out).isInstanceOf(MoveOutcome.Done.class);
        String archPath = "/" + root + "/" + runbooks.id() + "/" + arch.id() + "/";
        assertThat(out.node().path()).isEqualTo(archPath);
        assertThat(out.node().depth()).isEqualTo(2);
        assertThat(out.node().parentId()).isEqualTo(runbooks.id());
        assertThat(out.node().rank()).isGreaterThan(onCall.rank());   // appended after On-call
        for (Node adr : List.of(adr1, adr2)) {
            Node now = f.stored(adr.id());
            assertThat(now.path()).isEqualTo(archPath + adr.id() + "/");
            assertThat(now.depth()).isEqualTo(3);
            assertThat(now.parentId()).isEqualTo(arch.id());   // parent_id is untouched below the moved root
        }
        assertThat(f.stored(onCall.id()).path()).isEqualTo(onCall.path());
        assertThat(f.tree.ancestors(f.alice, adr2.id(), NONE)).extracting(Crumb::title)
                .containsExactly("ENG space", "Runbooks", "Architecture");
        assertThat(f.tree.space(f.alice, s.id(), NONE).treeVersion()).isEqualTo(1);
        assertThat(f.violations(s)).isEmpty();
    }

    @Test
    void childrenPageInSiblingOrderAndStayStableWhileInsertsHappen() {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            ids.add(f.page(s, root, "p" + i).id());
        }
        Page<Node> first = f.tree.children(f.alice, root, 3, null, NONE);
        assertThat(first.items()).extracting(Node::id).containsExactlyElementsOf(ids.subList(0, 3));
        assertThat(first.nextCursor()).isNotNull();

        // Inserted before the cursor: not seen again; after it: seen once, in place.
        f.tree.create(f.alice, s.id(), new CreateNode(root, "early", "page", null, ids.get(0), null), null);
        long late = f.tree.create(f.alice, s.id(), new CreateNode(root, "late", "page", ids.get(4), null, null), null)
                .value().id();

        List<Long> rest = new ArrayList<>();
        String cursor = first.nextCursor();
        while (cursor != null) {
            Page<Node> p = f.tree.children(f.alice, root, 3, cursor, NONE);
            p.items().forEach(n -> rest.add(n.id()));
            cursor = p.nextCursor();
        }
        assertThat(rest).containsExactly(ids.get(3), ids.get(4), late, ids.get(5), ids.get(6));
    }

    @Test
    void createBetweenSiblings() {
        Node a = f.page(s, root, "a");
        Node c = f.page(s, root, "c");
        Node b = f.tree.create(f.alice, s.id(), new CreateNode(root, "b", "page", a.id(), c.id(), null), null).value();
        assertThat(f.tree.children(f.alice, root, 50, null, NONE).items()).extracting(Node::title)
                .containsExactly("a", "b", "c");
        assertThat(b.rank()).isBetween(a.rank(), c.rank());
    }

    @Test
    void descendantsArePreOrderWithDepthLimitAndPaging() {
        Node a = f.page(s, root, "a");
        Node a1 = f.page(s, a.id(), "a1");
        Node a11 = f.page(s, a1.id(), "a11");
        Node a2 = f.page(s, a.id(), "a2");
        Node b = f.page(s, root, "b");

        assertThat(f.tree.descendants(f.alice, root, 10, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(a.id(), a1.id(), a11.id(), a2.id(), b.id());
        assertThat(f.tree.descendants(f.alice, root, 2, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(a.id(), a1.id(), a2.id(), b.id());
        assertThat(f.tree.descendants(f.alice, a.id(), 1, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(a1.id(), a2.id());

        List<Long> paged = new ArrayList<>();
        String cursor = null;
        do {
            Page<Node> p = f.tree.descendants(f.alice, root, 10, 2, cursor, NONE);
            p.items().forEach(n -> paged.add(n.id()));
            cursor = p.nextCursor();
        } while (cursor != null);
        assertThat(paged).containsExactly(a.id(), a1.id(), a11.id(), a2.id(), b.id());

        assertThatThrownBy(() -> f.tree.descendants(f.alice, root, 0, 50, null, NONE))
                .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    void staleVersionIsRejected() {
        Node a = f.page(s, root, "a");
        Node renamed = f.tree.update(f.alice, a.id(), a.version(), "a2", "hello").value();
        assertThat(renamed.version()).isEqualTo(a.version() + 1);
        assertThat(f.tree.get(f.alice, a.id(), true, NONE).body()).isEqualTo("hello");

        assertThatThrownBy(() -> f.tree.update(f.alice, a.id(), a.version(), "a3", null))
                .isInstanceOf(StaleVersionException.class);
        assertThatThrownBy(() -> f.tree.trash(f.alice, a.id(), a.version()))
                .isInstanceOf(StaleVersionException.class);
        assertThatThrownBy(() -> f.tree.move(f.alice, a.id(), new MoveNode(f.page(s, root, "b").id(), null, null),
                (long) a.version())).isInstanceOf(StaleVersionException.class);
    }

    @Test
    void reorderChangesOnlyTheRank() {
        Node a = f.page(s, root, "a");
        Node b = f.page(s, root, "b");
        Node c = f.page(s, root, "c");
        long treeVersion = f.tree.space(f.alice, s.id(), NONE).treeVersion();

        Node moved = f.tree.move(f.alice, c.id(), new MoveNode(root, null, a.id()), null).value().node();

        assertThat(moved.path()).isEqualTo(c.path());
        assertThat(f.tree.children(f.alice, root, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(c.id(), a.id(), b.id());
        assertThat(f.tree.space(f.alice, s.id(), NONE).treeVersion()).isEqualTo(treeVersion);
    }

    @Test
    void cyclesAndRootMovesAreRejected() {
        Node a = f.page(s, root, "a");
        Node a1 = f.page(s, a.id(), "a1");
        Node a11 = f.page(s, a1.id(), "a11");

        assertThatThrownBy(() -> f.tree.move(f.alice, a.id(), new MoveNode(a11.id(), null, null), null))
                .isInstanceOf(ConflictException.class).hasMessageContaining("cycle");
        assertThatThrownBy(() -> f.tree.move(f.alice, a.id(), new MoveNode(a.id(), null, null), null))
                .isInstanceOf(ConflictException.class);
        assertThatThrownBy(() -> f.tree.move(f.alice, root, new MoveNode(a.id(), null, null), null))
                .isInstanceOf(InvalidRequestException.class);
        assertThat(f.violations(s)).isEmpty();
    }

    @Test
    void depthIsLimited() {
        Node deepest = f.stored(root);
        while (deepest.depth() < Paths.MAX_DEPTH) {
            deepest = f.page(s, deepest.id(), "d" + (deepest.depth() + 1));
        }
        long last = deepest.id();
        assertThatThrownBy(() -> f.page(s, last, "too deep")).isInstanceOf(ConflictException.class);

        Node branch = f.page(s, root, "branch");
        f.page(s, branch.id(), "leaf");
        long parentOfLast = Objects.requireNonNull(deepest.parentId());
        // branch would land at depth 63 and its leaf at 64.
        assertThatThrownBy(() -> f.tree.move(f.alice, branch.id(), new MoveNode(parentOfLast, null, null), null))
                .isInstanceOf(ConflictException.class).hasMessageContaining("deeper");
    }

    @Test
    void typeRulesApply() {
        assertThatThrownBy(() -> f.create(s, root, "story", "story")).isInstanceOf(InvalidRequestException.class);
        Node epic = f.create(s, root, "epic", "epic");
        Node story = f.create(s, epic.id(), "story", "story");
        f.create(s, story.id(), "sub", "subtask");
        Node page = f.page(s, root, "page");
        assertThatThrownBy(() -> f.tree.move(f.alice, story.id(), new MoveNode(page.id(), null, null), null))
                .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    void trashHidesTheSubtreeAndRestoreBringsItBack() {
        Node a = f.page(s, root, "a");
        Node a1 = f.page(s, a.id(), "a1");
        Node b = f.page(s, root, "b");

        Node trashed = f.tree.trash(f.alice, a.id(), a.version()).value();
        assertThat(trashed.status()).isEqualTo(NodeStatus.TRASHED);
        assertThat(f.tree.children(f.alice, root, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(b.id());   // trashed pages leave the tree; the trash view lists them
        assertThat(f.tree.descendants(f.alice, root, 10, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(b.id());
        assertThat(f.tree.get(f.alice, a1.id(), false, NONE).inTrash()).isTrue();
        assertThatThrownBy(() -> f.tree.children(f.alice, a.id(), 50, null, NONE))
                .isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> f.page(s, a1.id(), "x")).isInstanceOf(ConflictException.class);
        assertThat(f.tree.trash(f.alice, s.id(), NONE)).extracting(Node::id).containsExactly(a.id());

        f.tree.restore(f.alice, a.id(), trashed.version());
        assertThat(f.tree.get(f.alice, a1.id(), false, NONE).inTrash()).isFalse();
        assertThat(f.tree.descendants(f.alice, root, 10, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(a.id(), a1.id(), b.id());
    }

    @Test
    void purgeDeletesTheTrashedSubtreeInBatches() {
        Node a = f.page(s, root, "a");
        for (int i = 0; i < 4; i++) {
            Node child = f.page(s, a.id(), "c" + i);
            f.page(s, child.id(), "g" + i);
        }
        Node keep = f.page(s, root, "keep");
        assertThatThrownBy(() -> f.tree.purge(f.alice, a.id(), a.version())).isInstanceOf(ConflictException.class);

        Node trashed = f.tree.trash(f.alice, a.id(), a.version()).value();
        long purged = f.tree.purge(f.alice, a.id(), trashed.version()).value();   // purgeBatchSize is 3

        assertThat(purged).isEqualTo(9);
        assertThatThrownBy(() -> f.tree.get(f.alice, a.id(), false, NONE)).isInstanceOf(NotFoundException.class);
        assertThat(f.tree.descendants(f.alice, root, 10, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(keep.id());
        assertThat(f.violations(s)).isEmpty();
    }

    @Test
    void restrictionsHideAndProtectASubtree() {
        Node secret = f.page(s, root, "secret");
        Node inner = f.page(s, secret.id(), "inner");
        Node open = f.page(s, root, "open");

        f.tree.setRestrictions(f.alice, secret.id(), RestrictionOp.VIEW, List.of("group:eng"));

        assertThatThrownBy(() -> f.tree.get(f.bob, inner.id(), false, NONE)).isInstanceOf(NotFoundException.class);
        assertThat(f.tree.children(f.bob, root, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(open.id());
        assertThat(f.tree.descendants(f.bob, root, 10, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(open.id());
        assertThat(f.tree.descendants(f.alice, root, 10, 50, null, NONE).items()).hasSize(3);

        String editor = "user:" + f.alice.actorId();
        f.tree.setRestrictions(f.alice, open.id(), RestrictionOp.EDIT, List.of(editor));
        assertThat(f.tree.get(f.bob, open.id(), false, NONE).node().id()).isEqualTo(open.id());
        assertThatThrownBy(() -> f.tree.update(f.bob, open.id(), open.version(), "mine", null))
                .isInstanceOf(ForbiddenException.class);
        f.page(s, open.id(), "child");   // the editor passes the edit restriction
        assertThat(f.tree.restrictions(f.alice, open.id(), NONE)).containsEntry(RestrictionOp.EDIT, List.of(editor));

        f.tree.setRestrictions(f.alice, secret.id(), RestrictionOp.VIEW, List.of());
        assertThat(f.tree.get(f.bob, inner.id(), false, NONE).node().id()).isEqualTo(inner.id());
    }

    @Test
    void idempotentCreateReplaysAndRejectsADifferentRequest() {
        CreateNode req = new CreateNode(root, "once", "page", null, null, null);
        Node first = f.tree.create(f.alice, s.id(), req, "key-0001").value();
        Node again = f.tree.create(f.alice, s.id(), req, "key-0001").value();

        assertThat(again.id()).isEqualTo(first.id());
        assertThat(f.tree.children(f.alice, root, 50, null, NONE).items()).hasSize(1);
        assertThatThrownBy(() -> f.tree.create(f.alice, s.id(),
                new CreateNode(root, "twice", "page", null, null, null), "key-0001"))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void spaceKeysAreUniquePerTenant() {
        assertThatThrownBy(() -> f.space("ENG")).isInstanceOf(ConflictException.class);
        new Fixture(10).space("ENG");   // another tenant
    }

    @Test
    void writesReachTheOutboxAndTheRelay() {
        Node a = f.page(s, root, "a");
        Node b = f.page(s, root, "b");
        f.tree.move(f.alice, b.id(), new MoveNode(a.id(), null, null), null);
        f.relay.relayAll();

        assertThat(f.events).filteredOn(e -> e.tenantId().equals(f.tenant)).extracting(TreeEvent::type)
                .containsExactly(TreeEvent.Type.SPACE_CREATED, TreeEvent.Type.NODE_CREATED,
                        TreeEvent.Type.NODE_CREATED, TreeEvent.Type.NODE_MOVED);
        assertThat(f.events).filteredOn(e -> e.tenantId().equals(f.tenant)).extracting(TreeEvent::seq)
                .isSorted();
    }

    /** DESIGN.md §12 "path or parent drift": the verifier finds it and repair rebuilds paths from parent_id. */
    @Test
    void repairRebuildsDriftedPaths() {
        Node a = f.page(s, root, "A");
        Node b = f.page(s, a.id(), "B");
        Node c = f.page(s, b.id(), "C");
        f.db.write(j -> j.update("UPDATE nodes SET path = '/1/2/' || node_id || '/', depth = 5 "
                + "WHERE tenant_id = ? AND node_id = ANY (?::bigint[])", f.tenant, "{" + b.id() + "," + c.id() + "}"));
        assertThat(f.violations(s)).hasSize(2);

        int fixed = f.db.write(j -> TreeVerifier.repair(j, f.tenant, s.id(), root, Duration.ofSeconds(2)));

        assertThat(fixed).isEqualTo(2);
        assertThat(f.violations(s)).isEmpty();
        assertThat(f.stored(c.id()).path()).isEqualTo(c.path());
        assertThat(f.stored(c.id()).depth()).isEqualTo(3);
    }
}
