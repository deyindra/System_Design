package com.salesforce.einstein.webcrawler.store;

import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStats;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.ResourceType;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * Jobs and each job's crawl graph: nodes ({@code job_pages}) and edges ({@code page_links}).
 * Everything is keyed by {@code job_id} first, so one job's graph is one partition range.
 *
 * <p>Atomicity contract (what makes a distributed crawl correct):
 * <ul>
 *   <li>{@link #admit} is the <b>seen-test</b>: an atomic insert-if-absent on {@code (job_id, url_hash)}, together with
 *       the budget counter. Two workers that discover the same URL concurrently: exactly one gets {@code ADMITTED}.
 *       (Any store with an atomic conditional insert can back it: a Lua script on a key-value cache, a lightweight
 *       transaction on a wide-column store, {@code INSERT … ON CONFLICT DO NOTHING} on a relational database.)</li>
 *   <li>{@link #claimContent} is the <b>content-seen test</b>, insert-if-absent on {@code (job_id, content_key)}.</li>
 *   <li>{@code pending} counts open tasks; the job is complete when it reaches zero. Tasks are opened and closed by
 *       key, each at most once, because the frontier delivers <i>at least</i> once (a crashed node's work is
 *       replayed elsewhere).</li>
 * </ul>
 * All per-job state lives here, not in an engine, so any number of engine nodes can work on one job.
 */
public interface JobStore {

    /** Creates the job, or returns the existing job with the same {@code (tenantId, idempotencyKey)}. */
    CrawlJob createOrGet(CrawlJob job);

    Optional<CrawlJob> get(String jobId);

    /** Compare-and-set on status. */
    boolean transition(String jobId, JobStatus from, JobStatus to, String error);

    /**
     * Notified on <i>every</i> node when a job reaches a terminal status, whoever made the transition (the
     * {@code job-done:{job}} channel). This is how a sync request waiting on one node learns that workers on other
     * nodes finished its crawl.
     */
    void onTerminal(Consumer<CrawlJob> listener);

    /**
     * Seen-test + budget. {@code enqueue}: the node will be fetched, so its task is opened ({@link #openTask}) under
     * {@code CrawlTask.key()}; otherwise it is recorded only (a referenced asset). A {@code REFERENCED} node is not
     * "seen" for an {@code enqueue} admission: it is upgraded to {@code QUEUED} and charged to the new task's budget,
     * so a URL first met as an embed can still be crawled when a page later links to it. A known page reached at a
     * smaller depth returns {@link Admission#SHALLOWER} after lowering its depth.
     */
    Admission admit(JobPage page, boolean enqueue, long budget);

    /**
     * Opens one unit of work: {@code pending++}, once per key. Retries use a new key; a duplicate open (the same
     * retry scheduled twice by a redelivered task) is a no-op, so it can't leave {@code pending} stuck above zero.
     *
     * @return {@code true} if the key was new (the caller should then produce the task)
     */
    boolean openTask(String jobId, String taskKey);

    /** {@code false} once the task is closed: a redelivered copy of finished work is dropped without side effects. */
    boolean isTaskOpen(String jobId, String taskKey);

    /**
     * Closes a task: {@code pending--}, at most once per key, however many times the log redelivers it.
     *
     * @return {@code pending} after the decrement, or empty if the key was not open (a duplicate)
     */
    OptionalLong closeTask(String jobId, String taskKey);

    /** Scope keys (hosts or registrable domains) of the job; a seed's redirect adds its destination. */
    void addScopeKey(String jobId, String key);

    boolean hasScopeKey(String jobId, String key);

    /** Spider-trap counter: query-string variants admitted for one host + path in this job. */
    long countVariant(String jobId, String pathKey);

    void update(JobPage page);

    Optional<JobPage> page(String jobId, String urlHash);

    /**
     * The tenant's most recent node with content for this URL, across all of the tenant's jobs. Backs "have
     * <i>we</i> crawled this?": a tenant sees only its own crawls, never that another tenant fetched a URL.
     */
    Optional<JobPage> latestForTenant(String tenantId, String urlHash);

    /**
     * @param contentKey the content hash plus whatever else must match for two URLs to be interchangeable (the engine
     *                   adds the directory, which relative links resolve against)
     * @return the URL that already owns this content in this job, or empty if {@code urlHash} now owns it
     */
    Optional<String> claimContent(String jobId, String contentKey, String urlHash);

    void addLink(LinkEdge edge);

    List<LinkEdge> outLinks(String jobId, String fromHash);

    /** Nodes in discovery (BFS) order. {@code type} {@code null} = all. */
    Slice<JobPage> pages(String jobId, ResourceType type, String cursor, int limit);

    /** A raw hold on {@code pending} that isn't a task (the guard while seeds are being added). */
    void incrementPending(String jobId);

    /** @return pending after the decrement */
    long decrementPending(String jobId);

    void markTruncated(String jobId);

    JobStats stats(String jobId);
}
