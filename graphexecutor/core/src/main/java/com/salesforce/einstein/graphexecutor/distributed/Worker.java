package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.ClusterStore.Arrival;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.Lease;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunState;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * One machine's share of a run over a graph that does not fit in one machine: the graph is in a
 * {@link GraphStore} (a graph database), and the run's shared state in a {@link ClusterStore}. Start one
 * worker per machine, each with the same executor class, run id and stores; they split the shards among
 * themselves and run the executor's rounds until the run is done.
 *
 * <p>The same algorithm as {@link DistributedTopologicalExecutor} and {@link DistributedTraversalExecutor}
 * (the same hooks run, with the same meaning), with two differences that come from being on many machines:
 * <ul>
 *   <li><b>Shards are leased.</b> A worker claims up to {@link Config#maxShards()} shards, and a heartbeat
 *       renews its leases every third of {@link Config#leaseTtl()}. A worker that dies stops renewing; when
 *       its leases expire, the others claim its shards (each up to its limit, more only if they stay
 *       unclaimed) and replay the round it was in.</li>
 *   <li><b>Nothing is kept between rounds</b> (see {@link ClusterJob}): a round's frontier comes from the
 *       previous round's messages in the cluster store, and finished work from the {@link CompletionLog}.
 *       So taking over a shard needs no state from the dead worker.</li>
 * </ul>
 *
 * <p>A round ends when every shard has arrived, and the run when a round sends nothing. Any worker
 * advances the run ({@link ClusterStore#advance} is a compare-and-set), so there is no coordinator to lose.
 * A shard round started {@value DistributedTraversalExecutor#MAX_ATTEMPTS} times fails the run, as a
 * partition that keeps crashing does in one JVM.
 *
 * <p>Hooks run on every worker, so they must be <b>deterministic</b> (the same answer on every machine) and
 * thread-safe (a worker runs its shards in parallel). An unchecked exception from a hook is this worker's
 * crash: {@link #run} rethrows it and the worker stops, without releasing its leases, as a machine that
 * died would.
 *
 * @param <T> node type
 */
public final class Worker<T> {

    /**
     * @param maxShards    the most shards this worker claims, unless some stay unclaimed for a whole lease
     * @param leaseTtl     how long a lease lasts without renewal: how soon a dead worker is replaced
     * @param pollInterval how long to wait when there is nothing to do yet
     * @param batchSize    nodes per read from the stores
     */
    public record Config(String runId, String workerId, int maxShards, Duration leaseTtl, Duration pollInterval,
                         int batchSize) {
        public Config {
            Objects.requireNonNull(runId, "runId");
            Objects.requireNonNull(workerId, "workerId");
            if (maxShards < 1 || batchSize < 1) {
                throw new IllegalArgumentException("maxShards and batchSize must be >= 1");
            }
            if (leaseTtl.isNegative() || leaseTtl.isZero() || pollInterval.isNegative()) {
                throw new IllegalArgumentException("leaseTtl must be > 0 and pollInterval >= 0");
            }
        }

        /** 4 shards, a 10 s lease, 100 ms polls, batches of 500. */
        public static Config of(String runId, String workerId) {
            return new Config(runId, workerId, 4, Duration.ofSeconds(10), Duration.ofMillis(100), 500);
        }

        public Config withMaxShards(int maxShards) {
            return new Config(runId, workerId, maxShards, leaseTtl, pollInterval, batchSize);
        }

        public Config withLease(Duration leaseTtl, Duration pollInterval) {
            return new Config(runId, workerId, maxShards, leaseTtl, pollInterval, batchSize);
        }

        public Config withBatchSize(int batchSize) {
            return new Config(runId, workerId, maxShards, leaseTtl, pollInterval, batchSize);
        }
    }

    private final ClusterJob<T> job;
    private final int shards;
    private final ClusterStore cluster;
    private final Config config;
    private final Map<Integer, Long> freeSince = new HashMap<>();   // shard -> when this worker first saw it free

    private Worker(ClusterJob<T> job, int shards, ClusterStore cluster, Config config) {
        this.job = job;
        this.shards = shards;
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * A worker of a breadth-first traversal (see {@link DistributedTraversalExecutor}). The executor's
     * {@code roots(Graph)} and {@code startRootlessGroups()} need the whole graph, so they are not asked:
     * traversal starts at {@code roots}, or at every node without predecessors when that is empty.
     */
    public static <T> Worker<T> traversal(DistributedTraversalExecutor<T> executor, Set<T> roots, GraphStore<T> graph,
                                          CompletionLog<T> log, NodeCodec<T> codec, ClusterStore cluster,
                                          Config config) {
        return new Worker<>(new TraversalJob<>(executor, roots, graph, log, codec, config.batchSize()),
                graph.shards(), cluster, config);
    }

    /**
     * A worker of a dependency-ordered run (see {@link DistributedTopologicalExecutor}). Nodes on a cycle,
     * and their dependants, never run; there is no up-front cycle check (it would read the whole graph).
     */
    public static <T> Worker<T> topological(DistributedTopologicalExecutor<T> executor, GraphStore<T> graph,
                                            CompletionLog<T> log, NodeCodec<T> codec, ClusterStore cluster,
                                            Config config) {
        return new Worker<>(new TopologicalJob<>(executor, graph, log, codec, config.batchSize()),
                graph.shards(), cluster, config);
    }

    /** Works on the run until it is done or failed, and returns its report. */
    public ClusterReport run() {
        String runId = config.runId();
        cluster.createRun(runId, shards, config.leaseTtl());
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(daemon("heartbeat"));
        long every = Math.max(1, config.leaseTtl().toMillis() / 3);
        heartbeat.scheduleAtFixedRate(() -> cluster.renew(runId, config.workerId(), config.leaseTtl()),
                every, every, TimeUnit.MILLISECONDS);
        ExecutorService pool = Executors.newFixedThreadPool(config.maxShards(), daemon("shard"));
        try {
            for (RunState state = state(); state.status() == RunStatus.RUNNING; state = state()) {
                int round = state.round();
                List<Integer> mine = claimShards();
                Set<Integer> arrived = cluster.arrived(runId, round);
                List<Integer> todo = mine.stream().filter(shard -> !arrived.contains(shard)).toList();
                if (!todo.isEmpty()) {
                    Supersteps.onEveryPartition(pool, todo.size(), i -> runShard(todo.get(i), round));
                }
                cluster.advance(runId, round);
                if (todo.isEmpty() && state().round() == round) {
                    sleep(config.pollInterval());
                }
            }
            return ClusterReport.read(cluster, runId);
        } finally {
            heartbeat.shutdownNow();
            pool.shutdownNow();
        }
    }

    private Void runShard(int shard, int round) {
        String runId = config.runId();
        int attempt = cluster.beginAttempt(runId, round, shard);
        if (attempt >= DistributedTraversalExecutor.MAX_ATTEMPTS) {
            cluster.fail(runId, "shard " + shard + " started " + (attempt + 1) + " times in round " + round);
            return null;
        }
        Arrival arrival = job.compute(cluster, runId, config.workerId(), shard, round, attempt);
        cluster.arrive(runId, arrival);   // false: the shard was taken over meanwhile; its new owner finishes it
        return null;
    }

    /**
     * Claims shards per the lease rules and returns the ones this worker now holds. A free shard (unowned or
     * expired) is taken while this worker is under {@link Config#maxShards()}, so a dead worker's shards,
     * which all expire together, spread over its replacements instead of going to whichever polls first.
     * Beyond the limit, a shard is taken only once it has stayed free for a whole lease: no worker under
     * its limit wanted it, and a run must not stall for want of workers.
     */
    private List<Integer> claimShards() {
        String runId = config.runId();
        List<Lease> leases = cluster.leases(runId);
        int held = (int) leases.stream().filter(this::holds).count();
        long now = System.nanoTime();
        List<Integer> mine = new ArrayList<>();
        for (Lease lease : leases) {
            if (holds(lease)) {
                freeSince.remove(lease.shard());
                mine.add(lease.shard());
                continue;
            }
            boolean free = lease.owner() == null || lease.expired();
            if (!free) {
                freeSince.remove(lease.shard());
                continue;
            }
            long since = freeSince.computeIfAbsent(lease.shard(), shard -> now);
            boolean abandoned = now - since >= config.leaseTtl().toNanos();
            if ((held < config.maxShards() || abandoned)
                    && cluster.claim(runId, lease.shard(), config.workerId(), config.leaseTtl())) {
                freeSince.remove(lease.shard());
                held++;
                mine.add(lease.shard());
            }
        }
        return mine;
    }

    private boolean holds(Lease lease) {
        return config.workerId().equals(lease.owner()) && !lease.expired();
    }

    private RunState state() {
        return cluster.run(config.runId()).orElseThrow(() -> new IllegalStateException("no run " + config.runId()));
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private ThreadFactory daemon(String role) {
        return task -> {
            Thread thread = new Thread(task, config.workerId() + "-" + role);
            thread.setDaemon(true);
            return thread;
        };
    }
}
