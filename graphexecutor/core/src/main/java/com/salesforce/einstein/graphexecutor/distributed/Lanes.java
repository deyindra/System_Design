package com.salesforce.einstein.graphexecutor.distributed;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Politeness within one partition: tasks of one lane start at least a delay apart, and lanes take turns so
 * that one slow lane does not hold back the others' first tasks. Not thread-safe: one partition, one thread.
 */
final class Lanes {
    private final Map<Object, Long> lastStart = new HashMap<>();   // when each lane last started a task

    /** {@code nodes} reordered: first node of every lane, then second of every lane, and so on. */
    static <T> List<T> roundRobin(List<T> nodes, Function<? super T, ?> laneOf) {
        Map<Object, Deque<T>> lanes = new LinkedHashMap<>();
        for (T node : nodes) {
            lanes.computeIfAbsent(laneOf.apply(node), lane -> new ArrayDeque<>()).add(node);
        }
        List<T> ordered = new ArrayList<>(nodes.size());
        while (!lanes.isEmpty()) {
            lanes.values().removeIf(lane -> {
                ordered.add(lane.poll());
                return lane.isEmpty();
            });
        }
        return ordered;
    }

    /** Waits until {@code delayNanos} after the lane's previous start, then marks a start now. */
    void awaitTurn(Object lane, long delayNanos) {
        Long last = lastStart.get(lane);
        if (last != null && delayNanos > 0) {
            long wait = last + delayNanos - System.nanoTime();
            if (wait > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for lane " + lane, e);
                }
            }
        }
        lastStart.put(lane, System.nanoTime());
    }
}
