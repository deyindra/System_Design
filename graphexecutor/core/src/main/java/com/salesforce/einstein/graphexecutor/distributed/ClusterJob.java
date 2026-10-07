package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.ClusterStore.Arrival;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * One shard's round in a {@link Worker}: what a {@link Partition} or {@link TraversalPartition} does in one
 * superstep, but over a {@link GraphStore} and with <b>no state kept between rounds</b>.
 *
 * <p>A round's frontier is read back from the previous round's messages, which the {@link ClusterStore}
 * keeps; finished work is read from the {@link CompletionLog}. So a shard's round is a function of
 * durable state only, and replaying it on another machine after a crash is the same call:
 * <pre>reached(0) = the shard's roots      reached(r) = inbox(r - 1)
 * frontier(r) = { v in reached(r) : v has no outcome before r, and ready(v, r) }</pre>
 * where {@code ready} is the executor's rule (every predecessor done, for a topological run; admitted, for
 * a traversal). Nodes of the frontier with an outcome at round r itself were run by a crashed attempt:
 * the outcome is reused, not run again, so the replay sends exactly what the crashed attempt did.
 *
 * <p>Memory is the frontier and its edges, never the shard: edges and outcomes are read in batches of
 * {@code batchSize}.
 *
 * @param <T> node type
 */
abstract class ClusterJob<T> {
    final GraphStore<T> store;
    final CompletionLog<T> log;
    private final NodeCodec<T> codec;
    final int batchSize;

    ClusterJob(GraphStore<T> store, CompletionLog<T> log, NodeCodec<T> codec, int batchSize) {
        this.store = store;
        this.log = log;
        this.codec = codec;
        this.batchSize = batchSize;
    }

    /** The shard's round-0 nodes. */
    abstract List<T> roots(int shard);

    /** Of the reached, unfinished nodes, the ones that run this round. */
    abstract List<T> ready(List<T> reached, int round);

    /** Runs one node and logs its outcome (logging before any message for it is sent). */
    abstract Status run(T node, int round, Lanes lanes);

    /** Whether a node with this outcome sends to its successors. */
    abstract boolean expands(Status status);

    /** The executor's fault-injection hook, called after each batch sent. */
    abstract void afterSend(int shard, int round, int attempt);

    /** The order the frontier runs in. Default: as given. */
    List<T> order(List<T> frontier) {
        return frontier;
    }

    Arrival compute(ClusterStore cluster, String runId, String worker, int shard, int round, int attempt) {
        List<T> reached = round == 0 ? roots(shard)
                : cluster.inbox(runId, round - 1, shard).stream().map(codec::decode).toList();
        Map<T, Outcome> logged = batched(reached, log::outcomes);
        List<T> unfinished = reached.stream()
                .filter(node -> !logged.containsKey(node) || logged.get(node).round() >= round)
                .toList();

        long done = 0;
        long failed = 0;
        long skipped = 0;
        List<T> expanding = new ArrayList<>();
        Lanes lanes = new Lanes();
        for (T node : order(ready(unfinished, round))) {
            Outcome replayed = logged.get(node);
            Status status = replayed != null ? replayed.status() : run(node, round, lanes);
            switch (status) {
                case DONE -> done++;
                case FAILED -> failed++;
                case SKIPPED -> skipped++;
            }
            if (expands(status)) {
                expanding.add(node);
            }
        }

        Map<Integer, Set<String>> outgoing = new TreeMap<>();   // receiving shard -> combined batch
        long edges = 0;
        for (List<T> successors : batched(expanding, store::successors).values()) {
            for (T successor : successors) {
                outgoing.computeIfAbsent(store.shardOf(successor), s -> new LinkedHashSet<>())
                        .add(codec.encode(successor));
                edges++;
            }
        }
        for (Map.Entry<Integer, Set<String>> batch : outgoing.entrySet()) {
            cluster.send(runId, round, shard, batch.getKey(), batch.getValue());
            afterSend(shard, round, attempt);
        }
        return new Arrival(round, shard, worker, attempt, done, failed, skipped, outgoing.size(), edges);
    }

    /** Calls {@code read} on {@code nodes} in batches of {@code batchSize} and merges the answers. */
    <V> Map<T, V> batched(Collection<T> nodes, Function<List<T>, Map<T, V>> read) {
        Map<T, V> merged = new HashMap<>();
        List<T> all = List.copyOf(nodes);
        for (int from = 0; from < all.size(); from += batchSize) {
            merged.putAll(read.apply(all.subList(from, Math.min(all.size(), from + batchSize))));
        }
        return merged;
    }

    /** Pages through the shard's sources. */
    List<T> allSources(int shard) {
        List<T> sources = new ArrayList<>();
        for (List<T> page = store.sources(shard, null, batchSize); !page.isEmpty();
             page = store.sources(shard, page.get(page.size() - 1), batchSize)) {
            sources.addAll(page);
        }
        return sources;
    }

    /** Sorts by key, so every attempt of a round sees the same order. */
    List<T> byKey(Collection<T> nodes) {
        List<T> sorted = new ArrayList<>(nodes);
        sorted.sort(Comparator.comparing(codec::encode));
        return sorted;
    }
}
