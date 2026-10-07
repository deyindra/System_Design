package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.ClusterStore.Arrival;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunState;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * How a {@link Worker} run went, from the arrivals in the {@link ClusterStore}: totals only, because a graph
 * that needs a cluster has too many nodes to list. Each node's outcome is in the {@link CompletionLog}.
 *
 * @param rounds     rounds that ran (the last one sent nothing)
 * @param recoveries shard rounds replayed after their worker died
 * @param workers    the workers that finished at least one shard round
 * @param error      why the run failed; null unless {@code FAILED}
 */
public record ClusterReport(String runId, RunStatus status, int rounds, long done, long failed, long skipped,
                            long batches, long edges, long recoveries, Set<String> workers, String error) {

    /** The run's report as it stands: any process that reaches the cluster store can read it, not only workers. */
    public static ClusterReport read(ClusterStore cluster, String runId) {
        RunState state = cluster.run(runId).orElseThrow(() -> new IllegalArgumentException("no run " + runId));
        return of(runId, state, cluster.arrivals(runId));
    }

    static ClusterReport of(String runId, RunState state, List<Arrival> arrivals) {
        Set<String> workers = new TreeSet<>();
        long done = 0;
        long failed = 0;
        long skipped = 0;
        long batches = 0;
        long edges = 0;
        long recoveries = 0;
        int rounds = 0;
        for (Arrival arrival : arrivals) {
            workers.add(arrival.worker());
            done += arrival.done();
            failed += arrival.failed();
            skipped += arrival.skipped();
            batches += arrival.batches();
            edges += arrival.edges();
            recoveries += arrival.attempt() > 0 ? 1 : 0;
            rounds = Math.max(rounds, arrival.round() + 1);
        }
        return new ClusterReport(runId, state.status(), rounds, done, failed, skipped, batches, edges, recoveries,
                Set.copyOf(workers), state.error());
    }
}
