package com.salesforce.einstein.hierarchy.cache;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Redis-backed {@link TreeCache} with JSON values. Keys carry the tenant as a hash tag ({@code ti:{tenant}:42}), so a
 * breadcrumb's crumbs are fetched with one MGET even on Redis Cluster.
 *
 * <p>{@code tv} keys are only ever raised (a Lua compare-and-set) and outlive every entry that refers to them: each
 * entry is written right after its {@code tv} is raised and refreshed, and {@code tv} lives twice as long. A replica
 * that lags therefore can't lower {@code tv} and bring old entries back to life.
 */
public final class RedisTreeCache implements TreeCache {
    private static final Logger log = LoggerFactory.getLogger(RedisTreeCache.class);
    private static final String RAISE = """
            local cur = tonumber(redis.call('GET', KEYS[1]))
            if cur == nil or cur < tonumber(ARGV[1]) then
              redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
              return 1
            end
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return 0
            """;

    private final StatefulRedisConnection<String, String> connection;
    private final ObjectMapper json;
    private final Duration entryTtl;
    private final Duration nodeTtl;
    private final Duration timeout;

    /**
     * @param entryTtl lifetime of breadcrumbs and children pages; {@code tv} keys live twice as long
     * @param nodeTtl  lifetime of per-node crumbs (titles)
     */
    public RedisTreeCache(StatefulRedisConnection<String, String> connection, Duration entryTtl, Duration nodeTtl) {
        this.connection = connection;
        this.json = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                // Derived accessors such as Node.isRoot() are written but have no constructor parameter.
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
        this.entryTtl = entryTtl;
        this.nodeTtl = nodeTtl;
        this.timeout = connection.getTimeout();
    }

    @Override
    public OptionalLong treeVersion(UUID tenant, long spaceId) {
        try {
            String v = sync().get(tvKey(tenant, spaceId));
            return v == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(v));
        } catch (RedisException | NumberFormatException e) {
            return miss(e, OptionalLong.empty());
        }
    }

    @Override
    public void raiseTreeVersion(UUID tenant, long spaceId, long version) {
        try {
            sync().eval(RAISE, ScriptOutputType.INTEGER, new String[]{tvKey(tenant, spaceId)},
                    Long.toString(version), Long.toString(entryTtl.multipliedBy(2).toSeconds()));
        } catch (RedisException e) {
            miss(e, null);
        }
    }

    @Override
    public Optional<Breadcrumb> breadcrumb(UUID tenant, long nodeId) {
        return get(key("bc", tenant, nodeId), Breadcrumb.class);
    }

    @Override
    public void putBreadcrumb(UUID tenant, long nodeId, Breadcrumb value) {
        put(key("bc", tenant, nodeId), value, entryTtl);
    }

    @Override
    public Map<Long, Crumb> crumbs(UUID tenant, Collection<Long> nodeIds) {
        if (nodeIds.isEmpty()) {
            return Map.of();
        }
        try {
            List<Long> ids = List.copyOf(nodeIds);
            String[] keys = ids.stream().map(id -> key("ti", tenant, id)).toArray(String[]::new);
            Map<Long, Crumb> out = new HashMap<>();
            List<KeyValue<String, String>> values = sync().mget(keys);
            for (int i = 0; i < values.size(); i++) {
                KeyValue<String, String> kv = values.get(i);
                if (kv.hasValue()) {
                    out.put(ids.get(i), json.readValue(kv.getValue(), Crumb.class));
                }
            }
            return out;
        } catch (RedisException | JacksonException e) {
            return miss(e, Map.of());
        }
    }

    @Override
    public void putCrumbs(UUID tenant, Collection<Crumb> crumbs) {
        try {
            RedisAsyncCommands<String, String> async = connection.async();
            List<Future<?>> writes = new ArrayList<>(crumbs.size());
            SetArgs ex = SetArgs.Builder.ex(nodeTtl);
            for (Crumb c : crumbs) {
                writes.add(async.set(key("ti", tenant, c.id()), json.writeValueAsString(c), ex));
            }
            LettuceFutures.awaitAll(timeout, writes.toArray(new Future<?>[0]));
        } catch (RedisException | JacksonException e) {
            miss(e, null);
        }
    }

    @Override
    public Optional<ChildrenPage> children(UUID tenant, long parentId) {
        return get(key("ch", tenant, parentId), ChildrenPage.class);
    }

    @Override
    public void putChildren(UUID tenant, long parentId, ChildrenPage page) {
        put(key("ch", tenant, parentId), page, entryTtl);
    }

    @Override
    public void evictNode(UUID tenant, long nodeId, Collection<Long> parentIds) {
        List<String> keys = new ArrayList<>(parentIds.size() + 1);
        keys.add(key("ti", tenant, nodeId));
        parentIds.forEach(p -> keys.add(key("ch", tenant, p)));
        try {
            sync().del(keys.toArray(new String[0]));
        } catch (RedisException e) {
            miss(e, null);
        }
    }

    private <T> Optional<T> get(String key, Class<T> type) {
        try {
            String v = sync().get(key);
            return v == null ? Optional.empty() : Optional.of(json.readValue(v, type));
        } catch (RedisException | JacksonException e) {
            return miss(e, Optional.empty());
        }
    }

    private void put(String key, Object value, Duration ttl) {
        try {
            sync().set(key, json.writeValueAsString(value), SetArgs.Builder.ex(ttl));
        } catch (RedisException | JacksonException e) {
            miss(e, null);
        }
    }

    private RedisCommands<String, String> sync() {
        return connection.sync();
    }

    /** Cache trouble degrades to a miss; the database answers instead. */
    private static <T> T miss(Exception e, T fallback) {
        log.warn("tree cache unavailable: {}", e.toString());
        return fallback;
    }

    private static String tvKey(UUID tenant, long spaceId) {
        return key("tv", tenant, spaceId);
    }

    private static String key(String kind, UUID tenant, long id) {
        return kind + ":{" + tenant + "}:" + id;
    }
}
