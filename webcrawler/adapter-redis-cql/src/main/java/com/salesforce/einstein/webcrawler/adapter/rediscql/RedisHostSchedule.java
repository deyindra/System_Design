package com.salesforce.einstein.webcrawler.adapter.rediscql;

import com.salesforce.einstein.webcrawler.frontier.HostSchedule;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * Per-host "not before", shared by the fleet so a host's delay survives its partition moving to another node. A key
 * expires when its time passes, so the map only holds hosts that are cooling down. Not job-scoped: politeness is per
 * host, across jobs and tenants.
 */
public final class RedisHostSchedule implements HostSchedule, AutoCloseable {

    private final StatefulRedisConnection<String, String> conn;
    private final RedisCommands<String, String> redis;
    private final Clock clock;

    public RedisHostSchedule(RedisClient client, Clock clock) {
        this.conn = client.connect();
        this.redis = conn.sync();
        this.clock = clock;
    }

    private static String key(String host) { return "wc:host:" + host; }

    @Override public Optional<Instant> notBefore(String host) {
        String v = redis.get(key(host));
        return v == null ? Optional.empty() : Optional.of(Instant.ofEpochMilli(Long.parseLong(v)));
    }

    @Override public void put(String host, Instant notBefore) {
        long ms = notBefore.toEpochMilli();
        redis.set(key(host), Long.toString(ms), SetArgs.Builder.px(Math.max(1, ms - clock.millis())));
    }

    @Override public void close() { conn.close(); }
}
