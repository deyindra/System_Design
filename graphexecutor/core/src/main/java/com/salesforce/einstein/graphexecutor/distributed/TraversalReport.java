package com.salesforce.einstein.graphexecutor.distributed;

import java.util.List;
import java.util.Map;

/**
 * What a {@link DistributedTraversalExecutor} run did. Every node is in exactly one of: some round of
 * {@code rounds} (completed), {@code failed}, {@code excluded}, {@code tooDeep} or {@code unreachable}.
 *
 * @param rounds      nodes whose task completed in each round; round i holds nodes at depth i
 * @param depth       every reached node's shortest distance from a root (including too-deep ones)
 * @param failed      nodes whose task threw, with the error
 * @param excluded    nodes the executor's {@code admit} rejected: never run, never traversed through
 * @param tooDeep     reached, but deeper than {@code maxDepth}: not run
 * @param unreachable admitted, but not reachable from any root through admitted nodes
 * @param stats       the cost of getting there
 */
public record TraversalReport<T>(List<List<T>> rounds, Map<T, Integer> depth, Map<T, Throwable> failed,
                                 List<T> excluded, List<T> tooDeep, List<T> unreachable, Stats stats) {

    /**
     * @param groups             weakly connected components found by label propagation
     * @param groupsPlacedWhole  groups owned entirely by one partition (their edges never cross)
     * @param groupsSplit        groups spread over more than one partition
     * @param edgeCut            edges whose endpoints live in different partitions
     * @param labelRounds        rounds label propagation took (about the largest group's diameter)
     * @param liveRounds         rounds of the traversal: the deepest shortest path plus one
     * @param visitEdges         edges traversed: the messages an uncombined design would send
     * @param batchesSent        messages actually sent: at most one per (sender, receiver, round)
     * @param duplicatesDropped  batches discarded by the receiver's dedupe
     * @param recoveries         times a crashed partition rebuilt its state from the completion log and replayed its round
     */
    public record Stats(int partitions, int groups, int groupsPlacedWhole, int groupsSplit, long edgeCut,
                        int labelRounds, int liveRounds, long visitEdges, long batchesSent,
                        long duplicatesDropped, int recoveries) {
    }
}
