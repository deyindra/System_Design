package com.salesforce.einstein.hierarchy.service;

import com.salesforce.einstein.hierarchy.domain.MoveJob;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import com.salesforce.einstein.hierarchy.spi.TreeEventListener;
import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.store.SpaceLocks;
import com.salesforce.einstein.hierarchy.store.TreeStore;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.CannotAcquireLockException;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Finishes large moves (DESIGN.md §8.5): rewrites the paths still under a job's old prefix in batches, then, under the
 * space's exclusive lock, marks the job DONE once no row is left there. Until then reads translate those rows through
 * the job's overlay, so the move already looks complete.
 *
 * <p>A job is held through a lease renewed with every batch. If this pod dies, another worker's sweep claims the job
 * when the lease runs out; batches are idempotent (a rewritten row leaves the old range), so nothing is done twice.
 */
public final class MoveJobWorker implements TreeEventListener, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MoveJobWorker.class);
    private static final long FINISH_RETRY_MS = 50;

    /**
     * @param batchSize       rows rewritten per transaction
     * @param lease           how long a claimed job stays ours without a renewal
     * @param maxReplicaLagMs pause between batches while the shard's replicas lag more than this
     * @param pause           how long to pause when they do
     * @param enabled         false in tests that drive {@link #runOnce()} themselves
     * @param lockTimeout     longest wait for the space lock at the finish step
     */
    public record Settings(int batchSize, Duration lease, long maxReplicaLagMs, Duration pause, boolean enabled,
                           Duration lockTimeout) {
    }

    private final ShardRouter shards;
    private final TreeStore store;
    private final TreeCache cache;
    private final Settings settings;
    private final String owner;
    private final ExecutorService executor;
    private final AtomicBoolean pending = new AtomicBoolean();
    private final AtomicBoolean scheduled = new AtomicBoolean();

    public MoveJobWorker(ShardRouter shards, TreeStore store, TreeCache cache, Settings settings, String owner) {
        this.shards = shards;
        this.store = store;
        this.cache = cache;
        this.settings = settings;
        this.owner = owner;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread th = new Thread(r, "move-job-worker");
            th.setDaemon(true);
            return th;
        });
    }

    /** Periodic: picks up jobs whose worker died (expired lease) or whose start event was missed. */
    public void sweep() {
        if (settings.enabled()) {
            kick();
        }
    }

    @Override
    public void onEvents(List<TreeEvent> events) {
        if (settings.enabled() && events.stream().anyMatch(e -> e.type() == TreeEvent.Type.MOVE_STARTED)) {
            kick();
        }
    }

    /** Runs {@link #runOnce()} on the worker thread; a kick while it runs makes it run once more. */
    private void kick() {
        pending.set(true);
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::drain);
        }
    }

    private void drain() {
        try {
            while (pending.getAndSet(false)) {
                runOnce();
            }
        } catch (RuntimeException e) {
            log.warn("move job sweep failed; the next sweep retries", e);
        } finally {
            scheduled.set(false);
            if (pending.get()) {
                kick();
            }
        }
    }

    /**
     * Claims and completes every claimable job on every shard, one at a time.
     *
     * @return the number of jobs completed
     */
    public int runOnce() {
        int completed = 0;
        for (Shard shard : shards.all()) {
            while (!Thread.currentThread().isInterrupted()) {
                var claimed = shard.primary().write(j -> store.claimJob(j, owner, settings.lease()));
                if (claimed.isEmpty()) {
                    break;
                }
                if (process(shard, claimed.get().tenantId(), claimed.get().job())) {
                    completed++;
                }
            }
        }
        return completed;
    }

    /** @return true if this worker completed the job; false if it lost the lease or was interrupted */
    private boolean process(Shard shard, UUID t, MoveJob job) {
        log.info("move job {}: rewriting {} -> {}", job.id(), job.oldPrefix(), job.newPrefix());
        long rows = job.rowsDone();
        while (!Thread.currentThread().isInterrupted()) {
            if (shard.replicas().maxLagMs() > settings.maxReplicaLagMs()) {
                // Each batch is replicated WAL; let the replicas catch up before adding more.
                if (!renew(shard, t, job) || interruptedWhileSleeping(settings.pause().toMillis())) {
                    return false;
                }
                continue;
            }
            int n = shard.primary().write(j -> {
                int r = store.rewriteBatch(j, t, job, settings.batchSize());
                return store.renewLease(j, t, job.id(), owner, settings.lease(), r) ? r : -1;
            });
            if (n < 0) {
                log.warn("move job {}: lease lost to another worker", job.id());
                return false;
            }
            rows += n;
            if (n == settings.batchSize()) {
                continue;
            }
            Boolean done = finish(shard, t, job);
            if (done == null) {
                return false;
            }
            if (done) {
                log.info("move job {}: done, {} rows rewritten", job.id(), rows);
                return true;
            }
            // Rows were skipped (locked by an edit) or the lock was busy: try again shortly.
            if (interruptedWhileSleeping(FINISH_RETRY_MS)) {
                return false;
            }
        }
        return false;
    }

    /**
     * Under the exclusive lock no create or move runs in the space, so "nothing left under the old prefix" stays true
     * until the overlay is gone.
     *
     * @return true when done, false to retry, null when the lease was lost
     */
    private Boolean finish(Shard shard, UUID t, MoveJob job) {
        try {
            Long tv = shard.primary().write(j -> {
                SpaceLocks.exclusive(j, t, List.of(job.spaceId()), settings.lockTimeout());
                if (!store.renewLease(j, t, job.id(), owner, settings.lease(), 0)) {
                    return -1L;
                }
                if (store.countUnder(j, t, job.oldPrefix(), 1) > 0) {
                    return 0L;
                }
                store.markDone(j, t, job.id());
                long v = store.bumpTreeVersion(j, t, job.spaceId());
                Long parent = store.node(j, t, job.rootNodeId()).map(Node::parentId).orElse(null);
                store.appendEvent(j, TreeEvent.moved(t, TreeEvent.Type.MOVE_COMPLETED, job.spaceId(), job.rootNodeId(),
                        parent, null, null, 0, job.id(), v));
                return v;
            });
            if (tv < 0) {
                return null;
            }
            if (tv == 0) {
                return false;
            }
            cache.raiseTreeVersion(t, job.spaceId(), tv);
            return true;
        } catch (CannotAcquireLockException e) {
            return false;
        }
    }

    /** Renews the lease without recording progress. */
    private boolean renew(Shard shard, UUID t, MoveJob job) {
        return shard.primary().write(j -> store.renewLease(j, t, job.id(), owner, settings.lease(), 0));
    }

    /** @return true if interrupted (the interrupt flag is restored), so the caller should stop */
    private static boolean interruptedWhileSleeping(long ms) {
        try {
            Thread.sleep(ms);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("move job worker did not stop within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
