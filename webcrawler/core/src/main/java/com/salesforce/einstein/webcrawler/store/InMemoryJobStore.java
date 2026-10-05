package com.salesforce.einstein.webcrawler.store;

import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlTask;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStats;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Single-process {@link JobStore}. Per-job state is guarded by that job's monitor. */
public final class InMemoryJobStore implements JobStore {

    private static final class JobData {
        volatile CrawlJob job;
        final Map<String, JobPage> nodes = new ConcurrentHashMap<>();
        final List<String> order = new ArrayList<>();              // discovery order, guarded by this
        final Map<String, String> contentOwner = new ConcurrentHashMap<>();
        final Map<String, List<LinkEdge>> out = new ConcurrentHashMap<>();
        final AtomicLong pending = new AtomicLong();
        final AtomicLong edges = new AtomicLong();
        final AtomicLong truncated = new AtomicLong();
        final Map<String, Boolean> tasks = new ConcurrentHashMap<>();   // task key → open
        final Set<String> scopeKeys = ConcurrentHashMap.newKeySet();
        final Map<String, AtomicLong> variants = new ConcurrentHashMap<>();
        long pageBudgetUsed, assetBudgetUsed;                      // guarded by this
        JobData(CrawlJob job) { this.job = job; }
    }

    private final Map<String, JobData> jobs = new ConcurrentHashMap<>();
    private final Map<String, String> byIdempotencyKey = new ConcurrentHashMap<>();
    /** tenant → url hash → latest node with content (in production: the {@code tenant_pages} table). */
    private final Map<String, Map<String, JobPage>> latestByTenant = new ConcurrentHashMap<>();
    private final List<Consumer<CrawlJob>> terminalListeners = new CopyOnWriteArrayList<>();

    @Override public CrawlJob createOrGet(CrawlJob job) {
        jobs.put(job.jobId(), new JobData(job));                  // before the key is published, so a loser can read it
        if (job.idempotencyKey() != null) {
            String key = job.tenantId() + "\u0000" + job.idempotencyKey();
            String existing = byIdempotencyKey.putIfAbsent(key, job.jobId());
            if (existing != null) {
                jobs.remove(job.jobId());
                return jobs.get(existing).job;
            }
        }
        return job;
    }

    @Override public Optional<CrawlJob> get(String jobId) {
        JobData d = jobs.get(jobId);
        return d == null ? Optional.empty() : Optional.of(d.job);
    }

    @Override public boolean transition(String jobId, JobStatus from, JobStatus to, String error) {
        JobData d = data(jobId);
        CrawlJob after;
        synchronized (d) {
            if (d.job.status() != from) return false;
            d.job = after = d.job.withStatus(to, Instant.now(), error);
        }
        if (to.isTerminal()) for (Consumer<CrawlJob> l : terminalListeners) {
            try { l.accept(after); } catch (RuntimeException ignored) { }
        }
        return true;
    }

    @Override public void onTerminal(Consumer<CrawlJob> listener) { terminalListeners.add(listener); }

    @Override public Admission admit(JobPage page, boolean enqueue, long budget) {
        JobData d = data(page.jobId());
        synchronized (d) {
            JobPage existing = d.nodes.get(page.urlHash());
            boolean upgrade = existing != null && enqueue && existing.status() == PageStatus.REFERENCED;
            if (existing != null && !upgrade) {
                if (!enqueue || existing.type().isAsset() || page.depth() >= existing.depth()) return Admission.SEEN;
                // Lower the depth on the current row, not on the copy read above: a worker may have just written
                // FETCHED, and putting back the old QUEUED copy would erase it.
                d.nodes.computeIfPresent(page.urlHash(), (k, cur) -> cur.atDepth(page.depth(), page.parentHash()));
                return Admission.SHALLOWER;
            }
            boolean asset = page.type().isAsset();
            long used = asset ? d.assetBudgetUsed : d.pageBudgetUsed;
            if (used >= budget) return Admission.OVER_BUDGET;
            if (asset) d.assetBudgetUsed++; else d.pageBudgetUsed++;
            d.nodes.put(page.urlHash(), page);
            if (!upgrade) d.order.add(page.urlHash());
            if (enqueue && d.tasks.putIfAbsent(CrawlTask.key(page.urlHash(), page.depth(), 0), true) == null)
                d.pending.incrementAndGet();
            return Admission.ADMITTED;
        }
    }

    @Override public void update(JobPage page) {
        JobData d = data(page.jobId());
        // Depth is owned by admit(): a worker writing back the copy it read before a shorter path arrived must not
        // raise it again (in CQL, update statements simply don't set the depth column).
        d.nodes.merge(page.urlHash(), page,
                (old, now) -> now.depth() > old.depth() ? now.atDepth(old.depth(), old.parentHash()) : now);
        if (page.status().hasContent())
            latestByTenant.computeIfAbsent(d.job.tenantId(), t -> new ConcurrentHashMap<>())
                    .merge(page.urlHash(), page, (old, now) -> now.fetchedAt().isBefore(old.fetchedAt()) ? old : now);
    }

    @Override public Optional<JobPage> page(String jobId, String urlHash) {
        JobData d = jobs.get(jobId);
        return d == null ? Optional.empty() : Optional.ofNullable(d.nodes.get(urlHash));
    }

    @Override public Optional<JobPage> latestForTenant(String tenantId, String urlHash) {
        return Optional.ofNullable(latestByTenant.getOrDefault(tenantId, Map.of()).get(urlHash));
    }

    @Override public Optional<String> claimContent(String jobId, String contentKey, String urlHash) {
        String owner = data(jobId).contentOwner.putIfAbsent(contentKey, urlHash);
        return owner == null || owner.equals(urlHash) ? Optional.empty() : Optional.of(owner);
    }

    @Override public void addLink(LinkEdge edge) {
        JobData d = data(edge.jobId());
        synchronized (d) { d.out.computeIfAbsent(edge.fromHash(), k -> new ArrayList<>()).add(edge); }
        d.edges.incrementAndGet();
    }

    @Override public List<LinkEdge> outLinks(String jobId, String fromHash) {
        JobData d = data(jobId);
        List<LinkEdge> l = d.out.get(fromHash);
        if (l == null) return List.of();
        synchronized (d) { return List.copyOf(l); }
    }

    /** The cursor is the position in discovery order: stable, since nodes are only appended. */
    @Override public Slice<JobPage> pages(String jobId, ResourceType type, String cursor, int limit) {
        JobData d = data(jobId);
        List<String> snapshot;
        synchronized (d) { snapshot = List.copyOf(d.order); }
        int from = cursor == null ? 0 : Integer.parseInt(cursor);
        List<JobPage> items = new ArrayList<>();
        int i = from;
        for (; i < snapshot.size() && items.size() < limit; i++) {
            JobPage p = d.nodes.get(snapshot.get(i));
            if (type == null || p.type() == type) items.add(p);
        }
        return new Slice<>(items, i < snapshot.size() ? Integer.toString(i) : null);
    }

    @Override public boolean openTask(String jobId, String taskKey) {
        JobData d = data(jobId);
        if (d.tasks.putIfAbsent(taskKey, true) != null) return false;
        d.pending.incrementAndGet();
        return true;
    }

    @Override public boolean isTaskOpen(String jobId, String taskKey) {
        return Boolean.TRUE.equals(data(jobId).tasks.get(taskKey));
    }

    @Override public OptionalLong closeTask(String jobId, String taskKey) {
        JobData d = data(jobId);
        return d.tasks.replace(taskKey, true, false) ? OptionalLong.of(d.pending.decrementAndGet()) : OptionalLong.empty();
    }

    @Override public void addScopeKey(String jobId, String key) { data(jobId).scopeKeys.add(key); }

    @Override public boolean hasScopeKey(String jobId, String key) { return data(jobId).scopeKeys.contains(key); }

    @Override public long countVariant(String jobId, String pathKey) {
        return data(jobId).variants.computeIfAbsent(pathKey, k -> new AtomicLong()).incrementAndGet();
    }

    @Override public void incrementPending(String jobId) { data(jobId).pending.incrementAndGet(); }

    @Override public long decrementPending(String jobId) { return data(jobId).pending.decrementAndGet(); }

    @Override public void markTruncated(String jobId) { data(jobId).truncated.incrementAndGet(); }

    @Override public JobStats stats(String jobId) {
        JobData d = data(jobId);
        long pages = 0, assets = 0, reused = 0, dups = 0, failed = 0;
        for (JobPage p : d.nodes.values()) {
            if (p.status().hasContent()) { if (p.type().isAsset()) assets++; else pages++; }
            if (p.status() == PageStatus.REUSED) reused++;
            if (p.status() == PageStatus.DUPLICATE) dups++;
            if (p.status() == PageStatus.FAILED || p.status() == PageStatus.TOO_LARGE) failed++;
        }
        return new JobStats(d.nodes.size(), d.pending.get(), pages, assets, reused, dups, failed,
                d.edges.get(), d.truncated.get());
    }

    private JobData data(String jobId) {
        JobData d = jobs.get(jobId);
        if (d == null) throw new IllegalArgumentException("unknown job " + jobId);
        return d;
    }
}
