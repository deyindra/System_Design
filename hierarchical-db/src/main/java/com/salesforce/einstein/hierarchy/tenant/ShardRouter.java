package com.salesforce.einstein.hierarchy.tenant;

import com.salesforce.einstein.hierarchy.store.Db;
import com.salesforce.einstein.hierarchy.store.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Finds a tenant's shard. All of a tenant's data lives on one shard, so no request spans two databases. */
public final class ShardRouter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ShardRouter.class);
    private final TenantDirectory directory;
    private final Map<String, Shard> shards = new LinkedHashMap<>();

    public ShardRouter(TenantDirectory directory, List<Shard> shards) {
        this.directory = directory;
        shards.forEach(s -> this.shards.put(s.name(), s));
    }

    public Shard shardFor(UUID tenant) {
        String name = directory.shardOf(tenant);
        Shard s = shards.get(name);
        if (s == null) {
            throw new IllegalStateException("tenant directory names unknown shard " + name);
        }
        return s;
    }

    public Collection<Shard> all() {
        return shards.values();
    }

    /** Closes every shard's connection pools (primary and replicas). */
    @Override
    public void close() {
        for (Shard s : shards.values()) {
            close(s.primary());
            s.replicas().replicas().forEach(ShardRouter::close);
        }
    }

    private static void close(Db db) {
        if (db.dataSource() instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("closing pool {} failed", db.name(), e);
            }
        }
    }
}
