package com.salesforce.einstein.tagging.service;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.TagEventListener;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Removes the assignments of a deleted tag in small transactions, so deleting a tag that sits on 10M
 * entities never holds a long lock or makes a huge WAL burst. Triggered by {@code TAG_DELETED}.
 * Reads already hide deleted tags, so the purge only reclaims space and keeps counters tidy.
 *
 * <p>Runs on its own executor, never on the event-delivery thread. A redelivered event purges again,
 * which is harmless.
 */
public final class TagPurgeJob implements TagEventListener {
    private static final Logger log = LoggerFactory.getLogger(TagPurgeJob.class);

    private final ShardRouter router;
    private final ExecutorService executor;
    private final int batchSize;
    private final Counter purged;

    public TagPurgeJob(ShardRouter router, ExecutorService executor, int batchSize, MeterRegistry metrics) {
        this.router = router;
        this.executor = executor;
        this.batchSize = batchSize;
        this.purged = metrics.counter("tagging.purge.assignments");
    }

    @Override
    public void onEvents(List<TagEvent> events) {
        for (TagEvent e : events) {
            if (e.type() == TagEvent.Type.TAG_DELETED) {
                executor.execute(() -> purge(e.tenantId(), e.tagId()));
            }
        }
    }

    /** Purges until the tag has no assignments left. Returns the number removed. */
    public long purge(String tenantId, long tagId) {
        long total = 0;
        try {
            int n;
            do {
                n = router.store(tenantId).write(tenantId, uow -> uow.purgeAssignments(tagId, batchSize));
                total += n;
                purged.increment(n);
            } while (n == batchSize);
            log.info("purged {} assignments of deleted tag {} (tenant {})", total, tagId, tenantId);
        } catch (RuntimeException ex) {
            log.warn("purge of tag {} (tenant {}) stopped after {}; a redelivery or the reconciler resumes it",
                    tagId, tenantId, total, ex);
        }
        return total;
    }
}
