package com.salesforce.einstein.tagging.store;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import com.salesforce.einstein.tagging.spi.TrendStore;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Single-process {@link TrendStore}: the reference implementation of the contract. One monitor serializes everything. */
public final class InMemoryTrendStore implements TrendStore {
    private record Cell(String tenantId, int grain, long bucket, long tagId) {
    }

    private record Progress(String tenantId, String shard) {
    }

    private final Map<Cell, Long> counts = new HashMap<>();
    private final Map<Progress, Long> progress = new HashMap<>();

    @Override
    public synchronized void record(String sourceShard, List<TagEvent> events) {
        for (TagEvent e : events) {
            Progress p = new Progress(e.tenantId(), sourceShard);
            if (e.seq() <= progress.getOrDefault(p, 0L)) {
                continue;
            }
            switch (e.type()) {
                case TAGS_ATTACHED -> {
                    for (int grain : TrendWindow.GRAINS) {
                        long bucket = TrendWindow.bucket(e.at(), grain);
                        e.tagIds().forEach(tag -> counts.merge(new Cell(e.tenantId(), grain, bucket, tag), 1L, Long::sum));
                    }
                }
                case TAG_DELETED -> counts.keySet().removeIf(c -> c.tenantId().equals(e.tenantId()) && c.tagId() == e.tagId());
                default -> {
                    continue;   // not counted, and doesn't move the progress (same as the JDBC store)
                }
            }
            progress.put(p, e.seq());
        }
    }

    @Override
    public synchronized List<TagCount> top(String tenantId, int grainSeconds, long firstBucket, long lastBucket,
                                           TrendRank rank, int limit) {
        long from = firstBucket - (lastBucket - firstBucket + 1);
        Map<Long, long[]> byTag = new HashMap<>();
        counts.forEach((c, n) -> {
            if (c.tenantId().equals(tenantId) && c.grain() == grainSeconds && c.bucket() >= from && c.bucket() <= lastBucket) {
                byTag.computeIfAbsent(c.tagId(), k -> new long[2])[c.bucket() >= firstBucket ? 0 : 1] += n;
            }
        });
        Comparator<TagCount> order = switch (rank) {
            case POPULAR -> Comparator.comparingLong(TagCount::count).reversed();
            case RISING -> Comparator.comparingLong((TagCount t) -> t.count() - t.previousCount()).reversed()
                    .thenComparing(Comparator.comparingLong(TagCount::count).reversed());
        };
        return byTag.entrySet().stream()
                .map(e -> new TagCount(e.getKey(), e.getValue()[0], e.getValue()[1]))
                .filter(t -> rank == TrendRank.POPULAR ? t.count() > 0 : t.count() > t.previousCount())
                .sorted(order.thenComparingLong(TagCount::tagId))
                .limit(limit)
                .toList();
    }

    @Override
    public synchronized int prune(int grainSeconds, long beforeBucket) {
        int before = counts.size();
        counts.keySet().removeIf(c -> c.grain() == grainSeconds && c.bucket() < beforeBucket);
        return before - counts.size();
    }
}
