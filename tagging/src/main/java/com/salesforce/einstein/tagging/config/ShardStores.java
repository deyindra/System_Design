package com.salesforce.einstein.tagging.config;

import com.salesforce.einstein.tagging.spi.Capability;
import com.salesforce.einstein.tagging.spi.TagStore;

import java.util.List;
import java.util.Map;

/** The store of every shard, plus whatever must be closed with them (connection pools). */
public record ShardStores(Map<String, TagStore> byShard, List<AutoCloseable> resources) implements AutoCloseable {
    /**
     * Fails startup unless every shard's store reports {@code capability}, so a weaker adapter can't
     * silently break a guarantee that {@code neededBy} relies on.
     */
    public void requireCapability(Capability capability, String neededBy) {
        byShard.forEach((shard, store) -> {
            if (!store.capabilities().contains(capability)) {
                throw new IllegalStateException("store " + store.getClass().getSimpleName() + " of shard " + shard
                        + " lacks " + capability + ", which " + neededBy + " requires");
            }
        });
    }

    @Override
    public void close() throws Exception {
        for (AutoCloseable r : resources) {
            r.close();
        }
    }
}
