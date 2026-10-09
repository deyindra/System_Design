package com.salesforce.einstein.hierarchy;

import com.salesforce.einstein.hierarchy.domain.ConflictException;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.Page;
import com.salesforce.einstein.hierarchy.domain.ReadToken;
import com.salesforce.einstein.hierarchy.domain.Space;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** DESIGN.md §8: the per-space lock keeps paths consistent under concurrent creates and moves. */
@Testcontainers(disabledWithoutDocker = true)
class ConcurrencyIT {
    private static final ReadToken NONE = ReadToken.NONE;
    private static final int CHAIN_LENGTH = 4;

    @Test
    void createsRacingSmallMovesKeepEveryPathConsistent() throws Exception {
        createsRacingMoves(new Fixture(10_000), false);
    }

    @Test
    void createsRacingLargeMovesKeepEveryPathConsistent() throws Exception {
        createsRacingMoves(new Fixture(3), true);
    }

    /**
     * 16 threads create under nodes of a subtree that 2 threads keep moving between two branches. Every create lands
     * either before or after each move, never halfway, so no row ends up with a path its parent doesn't have.
     */
    private static void createsRacingMoves(Fixture f, boolean drainJobs) throws Exception {
        Space s = f.space("RACE");
        long root = s.rootNodeId();
        Node left = chain(f, s, root, "left");
        Node right = chain(f, s, root, "right");
        Node mover = f.page(s, left.id(), "mover");
        List<Long> targets = new CopyOnWriteArrayList<>(List.of(mover.id(), f.page(s, mover.id(), "m1").id()));

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger moves = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(19);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            futures.add(pool.submit(() -> {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                while (!stop.get()) {
                    long parent = targets.get(r.nextInt(targets.size()));
                    if (retrying(() -> f.page(s, parent, "n"))) {
                        creates.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        for (int i = 0; i < 2; i++) {
            futures.add(pool.submit(() -> {
                while (!stop.get()) {
                    Node cur = f.stored(mover.id());
                    long to = Objects.equals(cur.parentId(), left.id()) ? right.id() : left.id();
                    if (retrying(() -> f.tree.move(f.alice, mover.id(), new MoveNode(to, null, null), null))) {
                        moves.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        ScheduledExecutorService ticker = Executors.newScheduledThreadPool(2);
        if (drainJobs) {
            ticker.scheduleWithFixedDelay(f.worker::runOnce, 0, 5, TimeUnit.MILLISECONDS);
        }
        // Grow the set of create targets with nodes that are themselves inside the moving subtree.
        CountDownLatch deep = new CountDownLatch(20);
        ticker.scheduleWithFixedDelay(() -> {
            if (deep.getCount() > 0
                    && retrying(() -> targets.add(f.page(s, targets.get(targets.size() - 1), "deep").id()))) {
                deep.countDown();
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
        assertThat(deep.await(30, TimeUnit.SECONDS)).isTrue();
        stop.set(true);
        ticker.shutdown();
        pool.shutdown();
        assertThat(ticker.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        for (Future<?> fu : futures) {
            fu.get();
        }
        int migrated;
        do {   // drain whatever job is still running
            migrated = f.worker.runOnce();
        } while (migrated > 0);

        assertThat(creates.get()).isPositive();
        assertThat(moves.get()).isPositive();
        assertThat(f.violations(s)).isEmpty();
        Node m = f.stored(mover.id());
        int subtree = f.db.read(j -> f.store.countUnder(j, f.tenant, m.path(), 1_000_000));
        assertThat(subtree).isEqualTo(2 + 20 + creates.get());
    }

    /** A and B moved under each other at the same moment: one wins, the other sees the cycle. Never both. */
    @Test
    void opposingMovesNeverFormACycle() throws Exception {
        Fixture f = new Fixture(10_000);
        Space s = f.space("CYCLE");
        long root = s.rootNodeId();
        Node a = f.page(s, root, "a");
        Node b = f.page(s, root, "b");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 25; round++) {
                for (Node n : List.of(a, b)) {
                    if (!Objects.equals(f.stored(n.id()).parentId(), root)) {
                        f.tree.move(f.alice, n.id(), new MoveNode(root, null, null), null);
                    }
                }
                CyclicBarrier go = new CyclicBarrier(2);
                Future<Boolean> ab = pool.submit(attempt(f, go, a.id(), b.id()));
                Future<Boolean> ba = pool.submit(attempt(f, go, b.id(), a.id()));
                boolean abWon = ab.get(10, TimeUnit.SECONDS);
                boolean baWon = ba.get(10, TimeUnit.SECONDS);
                assertThat(abWon && baWon).as("round %d: both moves succeeded", round).isFalse();
                assertThat(abWon || baWon).as("round %d: neither move succeeded", round).isTrue();
                assertThat(f.violations(s)).isEmpty();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static Callable<Boolean> attempt(Fixture f, CyclicBarrier go, long node, long under) {
        return () -> {
            go.await();
            try {
                f.tree.move(f.alice, node, new MoveNode(under, null, null), null);
                return true;
            } catch (ConflictException e) {
                assertThat(e.retryable()).isFalse();
                assertThat(e.getMessage()).contains("cycle");
                return false;
            }
        };
    }

    /** Appends share the space lock: none waits for another, and every one gets a place in one total order. */
    @Test
    void concurrentAppendsAllSucceed() throws Exception {
        Fixture f = new Fixture(10_000);
        Space s = f.space("APPEND");
        long root = s.rootNodeId();
        ExecutorService pool = Executors.newFixedThreadPool(32);
        List<Future<List<Long>>> futures = new ArrayList<>();
        for (int t = 0; t < 32; t++) {
            futures.add(pool.submit(() -> {
                List<Long> mine = new ArrayList<>();
                for (int i = 0; i < 50; i++) {
                    mine.add(f.page(s, root, "p" + i).id());
                }
                return mine;
            }));
        }
        Set<Long> created = new HashSet<>();
        for (Future<List<Long>> fu : futures) {
            created.addAll(fu.get(60, TimeUnit.SECONDS));
        }
        pool.shutdown();

        List<Node> all = new ArrayList<>();
        String cursor = null;
        do {
            Page<Node> p = f.tree.children(f.alice, root, 1000, cursor, NONE);
            all.addAll(p.items());
            cursor = p.nextCursor();
        } while (cursor != null);
        assertThat(created).hasSize(1600);
        assertThat(all).extracting(Node::id).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(created);
        for (int i = 1; i < all.size(); i++) {
            Node prev = all.get(i - 1);
            Node cur = all.get(i);
            int c = prev.rank().compareTo(cur.rank());
            assertThat(c < 0 || (c == 0 && prev.id() < cur.id())).isTrue();
        }
        assertThat(all).allSatisfy(n -> assertThat(n.rank().length()).isLessThanOrEqualTo(8));
    }

    private static Node chain(Fixture f, Space s, long parent, String name) {
        Node n = null;
        long p = parent;
        for (int i = 0; i < CHAIN_LENGTH; i++) {
            n = f.page(s, p, name + i);
            p = n.id();
        }
        return n;
    }

    /**
     * Runs {@code write}; a lock timeout or a retryable conflict counts as "didn't happen" (the client would retry).
     */
    private static boolean retrying(Runnable write) {
        try {
            write.run();
            return true;
        } catch (CannotAcquireLockException e) {
            return false;
        } catch (ConflictException e) {
            if (e.retryable()) {
                return false;
            }
            throw e;
        }
    }
}
