package com.salesforce.einstein.graphexecutor.distributed;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CompletionLog} in a concurrent map: it survives a partition's crash (it lives outside every
 * partition) but not the process. The stand-in for a durable store in this simulation.
 */
public final class InMemoryCompletionLog<T> implements CompletionLog<T> {
    private final Map<T, Outcome> outcomes = new ConcurrentHashMap<>();

    @Override
    public void record(T task, Outcome outcome) {
        outcomes.putIfAbsent(task, outcome);
    }

    @Override
    public Optional<Outcome> outcome(T task) {
        return Optional.ofNullable(outcomes.get(task));
    }
}
