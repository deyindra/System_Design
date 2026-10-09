package com.salesforce.einstein.hierarchy;

import com.salesforce.einstein.hierarchy.domain.ConflictException;
import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.domain.MoveJob;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.MoveOutcome;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DESIGN.md §8.5 with sync-limit 5: the overlay makes the move visible at once, then the worker catches up. */
@Testcontainers(disabledWithoutDocker = true)
class LargeMoveIT {
    private static final ReadToken NONE = ReadToken.NONE;

    private Fixture f;
    private Space s;
    private long root;
    private Node a;
    private Node b;
    /** The children c0 to c3 of {@code a}, each with one grandchild g0 to g3: 9 nodes counting {@code a}. */
    private final List<Node> children = new ArrayList<>();
    private final List<Node> grandchildren = new ArrayList<>();

    @BeforeEach
    void setUp() {
        f = new Fixture(5);
        s = f.space("BIG");
        root = s.rootNodeId();
        a = f.page(s, root, "a");
        for (int i = 0; i < 4; i++) {
            Node c = f.page(s, a.id(), "c" + i);
            children.add(c);
            grandchildren.add(f.page(s, c.id(), "g" + i));
        }
        b = f.page(s, root, "b");
    }

    @Test
    void readsShowTheMoveBeforeTheWorkerRuns() {
        MoveOutcome out = f.tree.move(f.alice, a.id(), new MoveNode(b.id(), null, null), null).value();

        assertThat(out).isInstanceOf(MoveOutcome.Accepted.class);
        MoveJob job = ((MoveOutcome.Accepted) out).job();
        String newPrefix = b.path() + a.id() + "/";
        assertThat(job.state()).isEqualTo(MoveJob.State.RUNNING);
        assertThat(job.oldPrefix()).isEqualTo(a.path());
        assertThat(job.newPrefix()).isEqualTo(newPrefix);
        assertThat(out.node().path()).isEqualTo(newPrefix);

        Node g0 = grandchildren.get(0);
        assertThat(f.stored(g0.id()).path()).startsWith(a.path());   // not rewritten yet
        Node seen = f.tree.get(f.alice, g0.id(), false, NONE).node();
        assertThat(seen.path()).isEqualTo(newPrefix + children.get(0).id() + "/" + g0.id() + "/");
        assertThat(seen.depth()).isEqualTo(4);
        assertThat(f.tree.ancestors(f.alice, g0.id(), NONE)).extracting(Crumb::id)
                .containsExactly(root, b.id(), a.id(), children.get(0).id());

        assertThat(f.tree.descendants(f.alice, b.id(), 10, 50, null, NONE).items()).hasSize(9)
                .allSatisfy(n -> assertThat(n.path()).startsWith(newPrefix));
        assertThat(f.tree.descendants(f.alice, root, 1, 50, null, NONE).items()).extracting(Node::id)
                .containsExactly(b.id());
        assertThat(f.tree.descendants(f.alice, root, 10, 50, null, NONE).items()).extracting(Node::id)
                .startsWith(b.id(), a.id(), children.get(0).id(), g0.id());
        assertThat(f.tree.children(f.alice, children.get(1).id(), 50, null, NONE).items()).singleElement()
                .satisfies(n -> assertThat(n.path()).startsWith(newPrefix));

        // A child created under a row the worker hasn't reached is born at its final path.
        Node born = f.page(s, children.get(2).id(), "born mid-move");
        assertThat(born.path()).isEqualTo(newPrefix + children.get(2).id() + "/" + born.id() + "/");
        assertThat(f.stored(born.id()).path()).isEqualTo(born.path());

        long x = f.page(s, root, "x").id();
        assertThatThrownBy(() -> f.tree.move(f.alice, x, new MoveNode(b.id(), null, null), null))
                .isInstanceOfSatisfying(ConflictException.class, e -> assertThat(e.retryable()).isTrue());
        assertThatThrownBy(() -> f.tree.purge(f.alice, a.id(), 1)).isInstanceOf(ConflictException.class);
    }

    @Test
    void theWorkerFinishesTheJob() {
        MoveJob job = ((MoveOutcome.Accepted) f.tree.move(f.alice, a.id(), new MoveNode(b.id(), null, null), null)
                .value()).job();
        long tvAtStart = f.tree.space(f.alice, s.id(), NONE).treeVersion();

        assertThat(f.worker.runOnce()).isGreaterThanOrEqualTo(1);

        MoveJob done = f.tree.moveJob(f.alice, job.id(), NONE);
        assertThat(done.state()).isEqualTo(MoveJob.State.DONE);
        assertThat(done.rowsDone()).isEqualTo(8);
        assertThat(done.finishedAt()).isNotNull();
        int left = f.db.read(j -> f.store.countUnder(j, f.tenant, job.oldPrefix(), 100));
        assertThat(left).isZero();
        assertThat(f.stored(grandchildren.get(3).id()).path()).startsWith(job.newPrefix());
        assertThat(f.stored(grandchildren.get(3).id()).depth()).isEqualTo(4);
        assertThat(f.tree.space(f.alice, s.id(), NONE).treeVersion()).isEqualTo(tvAtStart + 1);
        assertThat(f.violations(s)).isEmpty();

        // The space accepts moves again.
        assertThat(f.tree.move(f.alice, a.id(), new MoveNode(root, null, null), null).value())
                .isInstanceOf(MoveOutcome.Accepted.class);
        f.worker.runOnce();
        assertThat(f.violations(s)).isEmpty();

        f.relay.relayAll();
        assertThat(f.events).filteredOn(e -> e.tenantId().equals(f.tenant)).extracting(TreeEvent::type)
                .containsSubsequence(TreeEvent.Type.MOVE_STARTED, TreeEvent.Type.MOVE_COMPLETED,
                        TreeEvent.Type.MOVE_STARTED, TreeEvent.Type.MOVE_COMPLETED);
    }

    /** A row locked by a concurrent edit is skipped, and the job finishes only once that row is rewritten too. */
    @Test
    void lockedRowsAreRetriedBeforeTheJobFinishes() throws Exception {
        MoveJob job = ((MoveOutcome.Accepted) f.tree.move(f.alice, a.id(), new MoveNode(b.id(), null, null), null)
                .value()).job();
        long locked = grandchildren.get(1).id();
        CompletableFuture<Integer> run;
        try (Connection conn = Pg.dataSource().getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT 1 FROM nodes WHERE tenant_id = ? AND node_id = ? FOR UPDATE")) {
                ps.setObject(1, f.tenant);
                ps.setLong(2, locked);
                ps.executeQuery().close();
            }
            run = CompletableFuture.supplyAsync(f.worker::runOnce);
            Thread.sleep(500);
            assertThat(run).isNotDone();
            assertThat(f.stored(locked).path()).startsWith(job.oldPrefix());
            assertThat(f.stored(grandchildren.get(0).id()).path()).startsWith(job.newPrefix());
            conn.commit();
        }
        assertThat(run.get(10, TimeUnit.SECONDS)).isGreaterThanOrEqualTo(1);
        assertThat(f.stored(locked).path()).startsWith(job.newPrefix());
        assertThat(f.tree.moveJob(f.alice, job.id(), NONE).state()).isEqualTo(MoveJob.State.DONE);
    }

    @Test
    void largeMovesStayWithinOneSpace() {
        Space other = f.space("OTHER");
        assertThatThrownBy(() -> f.tree.move(f.alice, a.id(), new MoveNode(other.rootNodeId(), null, null), null))
                .isInstanceOf(ConflictException.class).hasMessageContaining("across spaces");

        // Small subtrees may cross: c0 and g0.
        Node c0 = children.get(0);
        Node moved = f.tree.move(f.alice, c0.id(), new MoveNode(other.rootNodeId(), null, null), null).value().node();
        assertThat(moved.spaceId()).isEqualTo(other.id());
        assertThat(f.stored(grandchildren.get(0).id()).spaceId()).isEqualTo(other.id());
        assertThat(f.violations(s)).isEmpty();
        assertThat(f.violations(other)).isEmpty();
    }
}
