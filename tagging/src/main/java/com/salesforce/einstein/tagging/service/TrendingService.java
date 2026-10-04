package com.salesforce.einstein.tagging.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import com.salesforce.einstein.tagging.domain.Trending;
import com.salesforce.einstein.tagging.spi.ReadPreference;
import com.salesforce.einstein.tagging.spi.TagReader;
import com.salesforce.einstein.tagging.spi.TenantIsolation;
import com.salesforce.einstein.tagging.spi.TenantIsolation.OpClass;
import com.salesforce.einstein.tagging.spi.TrendStore;
import com.salesforce.einstein.tagging.spi.TrendStore.TagCount;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * "Trending tags" for a tenant: the most attached (or fastest rising) tags over the last 24 h or 7 d.
 *
 * <p>This read is <b>always eventual</b>, by design. The counts are a projection of the event stream
 * (lagging the source of truth by the relay and consumer lag), the result is cached for
 * {@code tagging.trending.cache-ttl}, and tag metadata is read with EVENTUAL preference. None of it
 * touches the strong read path, so a popular-tags widget on every page costs the shard primaries
 * one small tag lookup per cache miss.
 */
public final class TrendingService {
    private static final Logger log = LoggerFactory.getLogger(TrendingService.class);

    private record Key(String tenantId, TrendWindow window, TrendRank rank, int limit) {
    }

    private final ShardRouter router;
    private final TenantIsolation isolation;
    private final TrendStore store;
    private final TaggingProperties.Trending cfg;
    private final Clock clock;
    private final Cache<Key, Trending> cache;

    public TrendingService(ShardRouter router, TenantIsolation isolation, TrendStore store,
                           TaggingProperties.Trending cfg, Clock clock) {
        this.router = router;
        this.isolation = isolation;
        this.store = store;
        this.cfg = cfg;
        this.clock = clock;
        this.cache = Caffeine.newBuilder().maximumSize(cfg.cacheMaxSize()).expireAfterWrite(cfg.cacheTtl()).build();
    }

    public Trending top(String tenantId, TrendWindow window, TrendRank rank, Integer limit) {
        int size = limit == null ? cfg.defaultLimit() : limit;
        if (size <= 0) {
            throw new InvalidRequestException("limit must be positive");
        }
        Key key = new Key(tenantId, window, rank, Math.min(size, cfg.maxLimit()));
        TenantInfo t = router.tenant(tenantId);
        return isolation.execute(t, OpClass.READ, 1, () -> cache.get(key, k -> load(t, k)));
    }

    private Trending load(TenantInfo t, Key k) {
        Instant now = clock.instant();
        // Over-fetch: a tag deleted within the consumer lag still has counters until TAG_DELETED is applied.
        List<TagCount> counts = store.top(k.tenantId(), k.window().grainSeconds(), k.window().firstBucket(now),
                k.window().lastBucket(now), k.rank(), k.limit() * 2);
        if (counts.isEmpty()) {
            return new Trending(k.window(), k.rank(), List.of(), now);
        }
        List<Long> ids = counts.stream().map(TagCount::tagId).toList();
        Map<Long, Tag> tags = router.store(t).read(k.tenantId(), ReadPreference.eventual(),
                (TagReader r) -> r.findTags(ids)).stream().filter(tag -> !tag.deleted())
                .collect(Collectors.toMap(Tag::tagId, Function.identity()));
        List<Trending.Item> items = new ArrayList<>(k.limit());
        for (TagCount c : counts) {
            Tag tag = tags.get(c.tagId());
            if (tag != null && items.size() < k.limit()) {
                items.add(new Trending.Item(tag, c.count(), c.previousCount()));
            }
        }
        return new Trending(k.window(), k.rank(), items, now);
    }

    /** Hourly housekeeping: drop buckets past retention. Idempotent, so every pod may run it. */
    @Scheduled(fixedDelayString = "${tagging.trending.prune-interval-ms:3600000}", initialDelay = 60_000)
    public void prune() {
        Instant now = clock.instant();
        try {
            int hourly = store.prune(TrendWindow.HOUR, TrendWindow.bucket(now.minus(cfg.hourlyRetention()), TrendWindow.HOUR));
            int daily = store.prune(TrendWindow.DAY, TrendWindow.bucket(now.minus(cfg.dailyRetention()), TrendWindow.DAY));
            log.info("pruned trending counters: {} hourly, {} daily", hourly, daily);
        } catch (RuntimeException e) {
            log.warn("pruning trending counters failed", e);
        }
    }
}
