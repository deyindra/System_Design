package com.salesforce.einstein.graphexecutor.executor;

import java.util.List;
import java.util.Map;

/**
 * What a {@link TraversalTaskExecutor} did with one group. Every node of the group is in exactly one of
 * {@code completed}, {@code failed}, {@code excluded}, {@code tooDeep} or {@code unreachable}.
 *
 * @param completed   nodes whose task returned normally, in completion order
 * @param depth       every reached node's shortest distance (edge count) from a root, in BFS order;
 *                    includes {@code tooDeep} nodes, excludes {@code excluded} and {@code unreachable} ones
 * @param failed      nodes whose task threw, with the error, in BFS order
 * @param excluded    nodes {@link TraversalTaskExecutor#admit} rejected: never run, never traversed through
 * @param tooDeep     reached, but deeper than {@link TraversalTaskExecutor#maxDepth}: not run
 * @param unreachable admitted, but no path of admitted nodes leads to them from a root
 */
public record TraversalResult<T>(List<T> completed, Map<T, Integer> depth, Map<T, Throwable> failed,
                                 List<T> excluded, List<T> tooDeep, List<T> unreachable) {
}
