package com.salesforce.einstein.graphexecutor.spi;

import com.salesforce.einstein.graphexecutor.distributed.Worker;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The shared state of a run whose partitions ({@link Worker}s) are on different machines: everything they
 * must agree on goes through here, so they never talk to each other directly. A database every worker can
 * reach implements it (the graph's own database, in practice); then the database is the network.
 *
 * <p>What it holds, per run:
 * <ul>
 *   <li><b>run state:</b> the current round, and whether the run is running, done or failed;</li>
 *   <li><b>leases:</b> which worker owns each shard, until when. A worker renews its leases while alive
 *       (its heartbeat); a lease that is not renewed expires, and another worker takes the shard over;</li>
 *   <li><b>messages:</b> the combined batch each shard sends each other shard in a round, as node keys;</li>
 *   <li><b>arrivals:</b> one per shard and round, written when the shard has finished the round. They are
 *       the barrier: the next round starts when every shard has arrived.</li>
 * </ul>
 *
 * <p>Rules every implementation follows:
 * <ul>
 *   <li><b>Time is the store's:</b> lease expiry uses the store's clock, never a worker's, so clock skew
 *       between machines cannot hand a shard to two owners;</li>
 *   <li><b>Writes are idempotent:</b> a second {@link #send} for the same (round, from, to) or a second
 *       {@link #arrive} for the same (round, shard) changes nothing. A replay re-sends the same batches, so
 *       duplicates are dropped by key, at no cost to the receiver;</li>
 *   <li><b>Arrivals are fenced:</b> {@link #arrive} succeeds only for the worker that holds the shard's
 *       unexpired lease, so a worker that was presumed dead (and replaced) cannot finish its old round;</li>
 *   <li><b>{@link #advance} is a compare-and-set:</b> any worker may call it any time, and only the first
 *       call that finds the round complete moves the run on. No coordinator is needed, so none can fail.</li>
 * </ul>
 */
public interface ClusterStore extends AutoCloseable {

    enum RunStatus { RUNNING, DONE, FAILED }

    /** @param error why the run failed; null unless {@code FAILED} */
    record RunState(int shards, int round, RunStatus status, String error) {
    }

    /** @param owner the worker holding the shard, null if none has yet; {@code expired} per the store's clock */
    record Lease(int shard, String owner, boolean expired) {
    }

    /**
     * What one shard did in one round.
     *
     * @param attempt 0 the first time the shard ran this round; more when it was replayed after its worker died
     * @param batches combined messages sent; a round in which no shard sends any is the last
     * @param edges   uncombined messages (one per edge) those batches stand for
     */
    record Arrival(int round, int shard, String worker, int attempt, long done, long failed, long skipped,
                   long batches, long edges) {
    }

    /**
     * Creates the run at round 0 unless it exists: every worker calls this, and the first one wins. Each
     * shard's lease starts unowned and expires after {@code ttl}, so shards that no worker claims in time
     * become claimable by any worker.
     */
    void createRun(String runId, int shards, Duration ttl);

    Optional<RunState> run(String runId);

    /** Every shard's lease, in shard order. */
    List<Lease> leases(String runId);

    /**
     * Takes the shard for {@code worker} until {@code ttl} from now, if it is unowned, expired, or already
     * the worker's.
     *
     * @return whether the worker now holds it
     */
    boolean claim(String runId, int shard, String worker, Duration ttl);

    /** The heartbeat: extends every unexpired lease that {@code worker} holds to {@code ttl} from now. */
    void renew(String runId, String worker, Duration ttl);

    /** Counts a start of the shard's round and returns how many starts preceded it (0 the first time). */
    int beginAttempt(String runId, int round, int shard);

    /** Stores the batch from shard {@code from} to shard {@code to}. Ignored if one is already stored. */
    void send(String runId, int round, int from, int to, Collection<String> nodes);

    /** Every node key sent to shard {@code to} in {@code round}, by any shard. */
    Set<String> inbox(String runId, int round, int to);

    /**
     * Records that a shard finished a round. Ignored if it already has.
     *
     * @return false if the worker no longer holds the shard: it has been replaced and must drop it
     */
    boolean arrive(String runId, Arrival arrival);

    /** The shards that have arrived in {@code round}. */
    Set<Integer> arrived(String runId, int round);

    /**
     * If the run is still at {@code round} and every shard has arrived: the run is {@code DONE} when no
     * shard sent anything, else it moves to round + 1. Otherwise does nothing.
     */
    void advance(String runId, int round);

    /** Marks the run failed, unless it has already finished. */
    void fail(String runId, String error);

    /** Every arrival of the run, ordered by round, then shard. */
    List<Arrival> arrivals(String runId);

    /** Releases connections. The default has none. */
    @Override
    default void close() {
    }
}
