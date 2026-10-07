package com.salesforce.einstein.graphexecutor.spi.memory;

import com.salesforce.einstein.graphexecutor.spi.ClusterStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A {@link ClusterStore} in memory, every method synchronized: workers that are threads of one JVM share it
 * as workers on many machines share a database. For tests, and for running the cluster protocol without a
 * database. The clock is injectable so a test can make leases expire.
 */
public final class InMemoryClusterStore implements ClusterStore {

    private static final class Run {
        RunState state;
        final Map<Integer, String> owner = new HashMap<>();
        final Map<Integer, Instant> expires = new HashMap<>();
        final Map<String, Integer> attempts = new HashMap<>();             // "round/shard" -> starts
        final Map<String, Set<String>> messages = new HashMap<>();         // "round/from/to" -> nodes
        final Map<Integer, Map<Integer, Arrival>> arrivals = new TreeMap<>(); // round -> shard -> arrival
    }

    private final Clock clock;
    private final Map<String, Run> runs = new HashMap<>();

    public InMemoryClusterStore() {
        this(Clock.systemUTC());
    }

    public InMemoryClusterStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public synchronized void createRun(String runId, int shards, Duration ttl) {
        if (runs.containsKey(runId)) {
            return;
        }
        Run run = new Run();
        run.state = new RunState(shards, 0, RunStatus.RUNNING, null);
        for (int s = 0; s < shards; s++) {
            run.expires.put(s, clock.instant().plus(ttl));
        }
        runs.put(runId, run);
    }

    @Override
    public synchronized Optional<RunState> run(String runId) {
        return Optional.ofNullable(runs.get(runId)).map(run -> run.state);
    }

    @Override
    public synchronized List<Lease> leases(String runId) {
        Run run = require(runId);
        List<Lease> leases = new ArrayList<>();
        for (int s = 0; s < run.state.shards(); s++) {
            leases.add(new Lease(s, run.owner.get(s), expired(run, s)));
        }
        return leases;
    }

    @Override
    public synchronized boolean claim(String runId, int shard, String worker, Duration ttl) {
        Run run = require(runId);
        String owner = run.owner.get(shard);
        if (owner != null && !owner.equals(worker) && !expired(run, shard)) {
            return false;
        }
        run.owner.put(shard, worker);
        run.expires.put(shard, clock.instant().plus(ttl));
        return true;
    }

    @Override
    public synchronized void renew(String runId, String worker, Duration ttl) {
        Run run = require(runId);
        run.owner.forEach((shard, owner) -> {
            if (owner.equals(worker) && !expired(run, shard)) {
                run.expires.put(shard, clock.instant().plus(ttl));
            }
        });
    }

    @Override
    public synchronized int beginAttempt(String runId, int round, int shard) {
        int before = require(runId).attempts.getOrDefault(round + "/" + shard, 0);
        require(runId).attempts.put(round + "/" + shard, before + 1);
        return before;
    }

    @Override
    public synchronized void send(String runId, int round, int from, int to, Collection<String> nodes) {
        require(runId).messages.putIfAbsent(round + "/" + from + "/" + to, new LinkedHashSet<>(nodes));
    }

    @Override
    public synchronized Set<String> inbox(String runId, int round, int to) {
        Run run = require(runId);
        Set<String> inbox = new TreeSet<>();
        for (int from = 0; from < run.state.shards(); from++) {
            inbox.addAll(run.messages.getOrDefault(round + "/" + from + "/" + to, Set.of()));
        }
        return inbox;
    }

    @Override
    public synchronized boolean arrive(String runId, Arrival arrival) {
        Run run = require(runId);
        if (!arrival.worker().equals(run.owner.get(arrival.shard())) || expired(run, arrival.shard())) {
            return false;   // fenced: the shard is someone else's now
        }
        run.arrivals.computeIfAbsent(arrival.round(), r -> new TreeMap<>()).putIfAbsent(arrival.shard(), arrival);
        return true;
    }

    @Override
    public synchronized Set<Integer> arrived(String runId, int round) {
        return Set.copyOf(require(runId).arrivals.getOrDefault(round, Map.of()).keySet());
    }

    @Override
    public synchronized void advance(String runId, int round) {
        Run run = require(runId);
        Map<Integer, Arrival> arrived = run.arrivals.getOrDefault(round, Map.of());
        if (run.state.status() != RunStatus.RUNNING || run.state.round() != round
                || arrived.size() < run.state.shards()) {
            return;
        }
        boolean quiet = arrived.values().stream().allMatch(a -> a.batches() == 0);
        run.state = quiet
                ? new RunState(run.state.shards(), round, RunStatus.DONE, null)
                : new RunState(run.state.shards(), round + 1, RunStatus.RUNNING, null);
    }

    @Override
    public synchronized void fail(String runId, String error) {
        Run run = require(runId);
        if (run.state.status() == RunStatus.RUNNING) {
            run.state = new RunState(run.state.shards(), run.state.round(), RunStatus.FAILED, error);
        }
    }

    @Override
    public synchronized List<Arrival> arrivals(String runId) {
        return require(runId).arrivals.values().stream()
                .flatMap(byShard -> byShard.values().stream())
                .sorted(Comparator.comparingInt(Arrival::round).thenComparingInt(Arrival::shard))
                .toList();
    }

    private boolean expired(Run run, int shard) {
        return !clock.instant().isBefore(run.expires.get(shard));
    }

    private Run require(String runId) {
        Run run = runs.get(runId);
        if (run == null) {
            throw new IllegalArgumentException("no run " + runId);
        }
        return run;
    }
}
