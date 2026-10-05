package com.salesforce.einstein.webcrawler.frontier;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** One process's {@link HostSchedule}: enough for tests and a single node, shared by nothing else. */
public final class InMemoryHostSchedule implements HostSchedule {

    private final Map<String, Instant> next = new ConcurrentHashMap<>();

    @Override public Optional<Instant> notBefore(String host) { return Optional.ofNullable(next.get(host)); }

    @Override public void put(String host, Instant notBefore) { next.put(host, notBefore); }
}
