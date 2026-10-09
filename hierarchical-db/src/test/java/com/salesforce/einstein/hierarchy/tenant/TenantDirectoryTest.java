package com.salesforce.einstein.hierarchy.tenant;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantDirectoryTest {
    @Test
    void explicitPlacementWins() {
        UUID whale = UUID.randomUUID();
        TenantDirectory d = new TenantDirectory(List.of("s0", "s1", "s2"), Map.of(whale, "s2"));
        assertThat(d.shardOf(whale)).isEqualTo("s2");
    }

    @Test
    void spreadsTenantsAndIsStable() {
        TenantDirectory d = new TenantDirectory(List.of("s0", "s1", "s2", "s3"), Map.of());
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 4000; i++) {
            UUID t = new UUID(i * 7919L, i * 104729L);
            String s = d.shardOf(t);
            assertThat(d.shardOf(t)).isEqualTo(s);
            counts.merge(s, 1, Integer::sum);
        }
        assertThat(counts.values()).allSatisfy(c -> assertThat(c).isBetween(800, 1200));
    }

    /** Rendezvous hashing: adding a fifth shard moves only the tenants that now land on it. */
    @Test
    void addingAShardMovesAboutOneFifth() {
        TenantDirectory four = new TenantDirectory(List.of("s0", "s1", "s2", "s3"), Map.of());
        TenantDirectory five = new TenantDirectory(List.of("s0", "s1", "s2", "s3", "s4"), Map.of());
        int moved = 0;
        for (int i = 0; i < 5000; i++) {
            UUID t = UUID.nameUUIDFromBytes(("tenant-" + i).getBytes());
            String before = four.shardOf(t);
            String after = five.shardOf(t);
            if (!before.equals(after)) {
                assertThat(after).isEqualTo("s4");
                moved++;
            }
        }
        assertThat(moved).isBetween(700, 1300);
    }

    @Test
    void rejectsUnknownShard() {
        assertThatThrownBy(() -> new TenantDirectory(List.of("s0"), Map.of(UUID.randomUUID(), "s9")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TenantDirectory(List.of(), Map.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
