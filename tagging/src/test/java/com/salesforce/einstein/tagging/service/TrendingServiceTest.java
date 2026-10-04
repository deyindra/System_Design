package com.salesforce.einstein.tagging.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.salesforce.einstein.tagging.cache.NoopDistributedCache;
import com.salesforce.einstein.tagging.cache.TagCache;
import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.Trending;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import com.salesforce.einstein.tagging.events.InProcessEventPublisher;
import com.salesforce.einstein.tagging.events.OutboxRelay;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.store.H2;
import com.salesforce.einstein.tagging.store.JdbcTagStore;
import com.salesforce.einstein.tagging.store.JdbcTrendStore;
import com.salesforce.einstein.tagging.tenant.Resilience4jTenantIsolation;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import com.salesforce.einstein.tagging.tenant.SnowflakeIdGenerator;
import com.salesforce.einstein.tagging.tenant.StaticTenantDirectory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Trending end to end below HTTP: writes → outbox → relay → aggregator → trend store → service. */
class TrendingServiceTest {
    private static final String T1 = "tenant-1";

    private final MutableClock clock = new MutableClock(Instant.parse("2026-03-10T09:15:00Z"));
    private TagService tags;
    private AssignmentService assignments;
    private TrendingService trending;
    private OutboxRelay relay;
    private int entity;

    @BeforeEach
    void setUp() {
        ObjectMapper json = new ObjectMapper().registerModule(new JavaTimeModule());
        TaggingProperties props = TaggingProperties.of(Map.of("trending.cache-ttl", "1m"));
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        TagStore store = new JdbcTagStore("s0", H2.newDatabase(), null, json, clock, 4);
        ShardRouter router = new ShardRouter(new StaticTenantDirectory(props), Map.of("s0", store));
        var isolation = new Resilience4jTenantIsolation(props, metrics);
        var ids = new SnowflakeIdGenerator(1, clock);
        TagCache cache = new TagCache(props.cache(), new NoopDistributedCache());
        JdbcTrendStore trends = new JdbcTrendStore(H2.newTrendDatabase(), clock);

        tags = new TagService(router, isolation, ids, cache, props.limits(), clock);
        assignments = new AssignmentService(router, isolation, cache, ids, new IdempotencyService(json, clock),
                props.limits(), clock);
        trending = new TrendingService(router, isolation, trends, props.trending(), clock);
        relay = new OutboxRelay(List.of(store), new InProcessEventPublisher(List.of(cache,
                new TrendingAggregator(trends, metrics))), props.relay(), clock, metrics);
    }

    private long tag(String name) {
        return tags.create(T1, "alice", name, null).value().tagId();
    }

    /** Attaches {@code tagId} to {@code entities} new entities (re-attaching is a no-op and emits nothing). */
    private void attach(long tagId, int entities) {
        for (int i = 0; i < entities; i++) {
            assignments.attach(T1, "alice", new EntityRef("jira:issue", Integer.toString(++entity)), List.of(tagId), null);
        }
    }

    private static List<String> names(Trending t) {
        return t.items().stream().map(i -> i.tag().name()).toList();
    }

    @Test
    @DisplayName("popular ranks live tags by attaches; a tag deleted within the lag is filtered out")
    void popular() {
        long bug = tag("bug");
        long ui = tag("ui");
        long old = tag("old");
        attach(bug, 3);
        attach(ui, 2);
        attach(old, 4);
        relay.relayAll();
        tags.delete(T1, "alice", old, null);   // not relayed yet: its counters are still there

        Trending t = trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 10);
        assertThat(names(t)).containsExactly("bug", "ui");
        assertThat(t.items().get(0).count()).isEqualTo(3);
        assertThat(t.asOf()).isEqualTo(clock.instant());
        assertThat(names(trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 1))).containsExactly("bug");
    }

    @Test
    @DisplayName("windows slide: yesterday's attaches leave the 24 h window but stay in 7 d and feed 'rising'")
    void windows() {
        long bug = tag("bug");
        long ui = tag("ui");
        attach(bug, 3);
        relay.relayAll();
        clock.advance(Duration.ofHours(25));
        attach(ui, 1);
        attach(bug, 1);
        relay.relayAll();

        Trending day = trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 10);
        assertThat(day.items()).extracting(i -> i.tag().name() + "=" + i.count() + "/" + i.previousCount())
                .containsExactly("bug=1/3", "ui=1/0");
        assertThat(names(trending.top(T1, TrendWindow.LAST_24H, TrendRank.RISING, 10))).containsExactly("ui");
        assertThat(trending.top(T1, TrendWindow.LAST_7D, TrendRank.POPULAR, 10).items())
                .extracting(i -> i.tag().name() + "=" + i.count()).containsExactly("bug=4", "ui=1");
    }

    @Test
    @DisplayName("results are cached for the TTL: an explicitly eventual read")
    void cached() {
        long bug = tag("bug");
        attach(bug, 1);
        relay.relayAll();
        Trending first = trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 10);
        attach(bug, 2);
        relay.relayAll();
        assertThat(trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 10)).isSameAs(first);
    }

    @Test
    @DisplayName("limit must be positive and is capped")
    void limits() {
        assertThatThrownBy(() -> trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 0))
                .isInstanceOf(InvalidRequestException.class);
        assertThat(trending.top(T1, TrendWindow.LAST_24H, TrendRank.POPULAR, 10_000).items()).isEmpty();
    }

    static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
