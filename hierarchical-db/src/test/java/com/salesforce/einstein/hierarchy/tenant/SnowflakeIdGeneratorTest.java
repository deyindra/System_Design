package com.salesforce.einstein.hierarchy.tenant;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnowflakeIdGeneratorTest {
    @Test
    void uniqueAndIncreasingWithinOneMillisecond() {
        Clock fixed = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        SnowflakeIdGenerator g = new SnowflakeIdGenerator(7, fixed);
        Set<Long> seen = new HashSet<>();
        long prev = -1;
        for (int i = 0; i < 10_000; i++) {   // more than 4096: borrows the following milliseconds
            long id = g.nextId();
            assertThat(id).isGreaterThan(prev);
            assertThat(seen.add(id)).isTrue();
            prev = id;
        }
    }

    @Test
    void workersNeverCollide() {
        Clock fixed = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        assertThat(new SnowflakeIdGenerator(1, fixed).nextId()).isNotEqualTo(new SnowflakeIdGenerator(2, fixed).nextId());
    }

    @Test
    void validatesWorkerId() {
        assertThatThrownBy(() -> new SnowflakeIdGenerator(1024, Clock.systemUTC()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SnowflakeIdGenerator(-1, Clock.systemUTC()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
