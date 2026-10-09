package com.salesforce.einstein.hierarchy.config;

import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.LinkedHashMap;
import java.util.Map;

/** {@code /actuator/health}: every shard primary must answer; replica lag is reported, not judged. */
public class ShardHealthIndicator implements HealthIndicator {
    private final ShardRouter shards;

    ShardHealthIndicator(ShardRouter shards) {
        this.shards = shards;
    }

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        boolean up = true;
        for (Shard s : shards.all()) {
            try {
                s.primary().jdbc().queryForObject("SELECT 1", Integer.class);
                details.put(s.name(), Map.of("primary", "UP", "replicas", s.replicas().replicas().size(),
                        "maxReplicaLagMs", s.replicas().maxLagMs()));
            } catch (RuntimeException e) {
                up = false;
                details.put(s.name(), Map.of("primary", "DOWN", "error", e.getClass().getSimpleName()));
            }
        }
        return (up ? Health.up() : Health.down()).withDetails(details).build();
    }
}
