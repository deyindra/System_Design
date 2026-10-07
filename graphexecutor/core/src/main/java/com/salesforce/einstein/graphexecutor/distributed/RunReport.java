package com.salesforce.einstein.graphexecutor.distributed;

import java.util.List;
import java.util.Map;

/**
 * What a {@link DistributedTopologicalExecutor} run did.
 *
 * @param rounds         nodes whose tasks completed in each round (round i only depends on rounds before i)
 * @param failed         nodes whose task threw, with the error
 * @param skipped        nodes never run because something they depend on failed
 * @param rejectedGroups under {@code ISOLATE_GROUP}: cyclic group id -> its nodes on or after a cycle
 *                       (none of the group ran)
 * @param stats          the cost of getting there
 */
public record RunReport<T>(List<List<T>> rounds, Map<T, Throwable> failed, List<T> skipped,
                           Map<Integer, List<T>> rejectedGroups, Stats stats) {

    /**
     * @param groups             weakly connected components found by label propagation
     * @param groupsPlacedWhole  groups owned entirely by one partition (their edges never cross)
     * @param groupsSplit        groups too big for one partition, split by the {@link Partitioner}
     * @param edgeCut            edges whose endpoints live in different partitions
     * @param labelRounds        rounds label propagation took (about the largest group's diameter)
     * @param dryRunRounds       rounds of the up-front cycle check
     * @param liveRounds         rounds of the real run (the longest dependency chain, plus one)
     * @param decrementEdges     dependencies released in the live run: messages an uncombined design would send
     * @param batchesSent        messages actually sent: at most one per (sender, receiver, round)
     * @param duplicatesDropped  batches discarded by the receiver's dedupe
     * @param recoveries         times a crashed partition rebuilt its state from the completion log and replayed its round
     */
    public record Stats(int partitions, int groups, int groupsPlacedWhole, int groupsSplit, long edgeCut,
                        int labelRounds, int dryRunRounds, int liveRounds, long decrementEdges,
                        long batchesSent, long duplicatesDropped, int recoveries) {
    }
}
