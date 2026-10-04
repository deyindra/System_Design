package com.salesforce.einstein.tagging.config;

import com.salesforce.einstein.tagging.spi.Capability;
import com.salesforce.einstein.tagging.store.InMemoryTagStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardStoresTest {
    private final ShardStores stores = new ShardStores(
            Map.of("s0", new InMemoryTagStore("s0", Clock.systemUTC())), List.of());

    @Test
    @DisplayName("startup fails when a shard's store lacks a capability a component needs")
    void requireCapability() {
        assertThatCode(() -> stores.requireCapability(Capability.ATOMIC_OUTBOX, "the outbox relay"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> stores.requireCapability(Capability.READ_REPLICAS, "replica reads"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shard s0 lacks READ_REPLICAS, which replica reads requires");
    }
}
