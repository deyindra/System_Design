package com.salesforce.einstein.webcrawler.frontier;

import com.salesforce.einstein.webcrawler.model.CrawlTask;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The distributed frontier, in one process: what a partitioned log (Kafka API) plus a consumer group gives the
 * cluster, written out so the tests can kill nodes. Production swaps in the log adapter; the engine sees the same
 * {@link Frontier} either way.
 *
 * <ul>
 *   <li><b>Host → partition:</b> a fixed hash of the host, like a producer's record key. A host's tasks are always in
 *       one partition, so one node owns the host and politeness is a local decision, not a distributed lock.</li>
 *   <li><b>Partition → node:</b> rendezvous (highest-random-weight) hashing over the live members. Adding or removing
 *       a node moves only the partitions it gains or loses, about {@code 1/N} of them; nothing else reshuffles.</li>
 *   <li><b>At-least-once:</b> a task stays in its partition's log until the node that took it calls
 *       {@link Frontier#release} (the commit). When a node leaves or dies, every uncommitted task of its partitions is
 *       replayed to the new owners, including the ones it was fetching. The engine makes the replay harmless
 *       ({@code JobStore.isTaskOpen} / {@code closeTask}).</li>
 *   <li><b>Politeness survives a handover:</b> each host's next-allowed time is kept with the log (the {@code hosts}
 *       table in production) and primed on the new owner, so a rebalance can't burst a host.</li>
 * </ul>
 * Each member runs an {@link InMemoryFrontier} over the partitions it owns: those are its back queues.
 */
public final class PartitionedFrontier {

    private record Entry(int partition, long offset, int epoch) { }

    private final int partitions;
    private final Clock clock;
    private final List<Map<Long, CrawlTask>> log = new ArrayList<>();    // per partition: uncommitted, offset order
    private final Map<String, Instant> hostNotBefore = new HashMap<>();
    private final Map<String, Member> members = new TreeMap<>();
    private final int[] epoch;
    private final Map<Integer, String> ownerNode = new HashMap<>();
    private long nextOffset;

    public PartitionedFrontier(int partitions, Clock clock) {
        if (partitions < 1) throw new IllegalArgumentException("partitions < 1");
        this.partitions = partitions;
        this.clock = clock;
        this.epoch = new int[partitions];
        for (int p = 0; p < partitions; p++) log.add(new LinkedHashMap<>());
    }

    /** Stable for the life of the topic; changing the partition count is a migration, like on a real log. */
    public int partitionOf(String host) {
        return (int) Long.remainderUnsigned(mix(fnv(host)), partitions);
    }

    /** Highest-random-weight owner of a partition among {@code nodes}. */
    static String rendezvous(int partition, Iterable<String> nodes) {
        String best = null;
        long bestWeight = 0;
        for (String n : nodes) {
            long w = mix(fnv(n) ^ (partition * 0x9E3779B97F4A7C15L));
            if (best == null || Long.compareUnsigned(w, bestWeight) > 0) { best = n; bestWeight = w; }
        }
        return best;
    }

    /** A node joins the group and gets its share of partitions (with their backlog). */
    public synchronized Frontier join(String nodeId) {
        if (members.containsKey(nodeId)) throw new IllegalArgumentException("already a member: " + nodeId);
        Member m = new Member(nodeId);
        members.put(nodeId, m);
        rebalance();
        return m;
    }

    /**
     * The node is gone, cleanly or not (a missed heartbeat looks the same). Its partitions move to the survivors and
     * every task it had not committed is delivered again there.
     */
    public synchronized void leave(String nodeId) {
        Member m = members.remove(nodeId);
        if (m == null) return;
        m.closed = true;
        Set<Integer> had = new HashSet<>(m.owned);
        rebalance();                                      // replays everything uncommitted in the partitions it owned
        for (Entry e : m.inFlight.values()) {             // and what it was still fetching from partitions it had lost
            CrawlTask t = log.get(e.partition).get(e.offset);
            Member now = members.get(ownerNode.get(e.partition));
            if (!had.contains(e.partition) && t != null && now != null)
                now.deliver(new Entry(e.partition, e.offset, epoch[e.partition]), t);
        }
    }

    /** node → partitions it owns. */
    public synchronized Map<String, Set<Integer>> assignment() {
        Map<String, Set<Integer>> out = new TreeMap<>();
        for (String n : members.keySet()) out.put(n, new HashSet<>());
        ownerNode.forEach((p, n) -> out.get(n).add(p));
        return out;
    }

    /** Uncommitted tasks across all partitions. */
    public synchronized long backlog() {
        return log.stream().mapToLong(Map::size).sum();
    }

    // ------------------------------------------------------------------ internals (all under this monitor)

    private synchronized void produce(CrawlTask t) {
        int p = partitionOf(t.host());
        long off = nextOffset++;
        log.get(p).put(off, t);
        Member m = members.get(ownerNode.get(p));
        if (m != null) m.deliver(new Entry(p, off, epoch[p]), t);
    }

    private synchronized boolean claim(Member m, Entry e) {
        return !m.closed && m.owned.contains(e.partition) && epoch[e.partition] == e.epoch
                && log.get(e.partition).containsKey(e.offset);
    }

    private synchronized void commit(Member from, Entry e, String host, Instant notBefore) {
        log.get(e.partition).remove(e.offset);
        hostNotBefore.merge(host, notBefore, (a, b) -> a.isAfter(b) ? a : b);
        // The partition moved while this fetch was running: tell the new owner when the host may be hit again.
        Member now = members.get(ownerNode.get(e.partition));
        if (now != null && now != from && !now.inFlight.containsKey(host)) now.queue.release(host, notBefore);
    }

    private void rebalance() {
        for (int p = 0; p < partitions; p++) {
            String next = members.isEmpty() ? null : rendezvous(p, members.keySet());
            String prev = ownerNode.get(p);
            if (next != null && next.equals(prev)) continue;
            Member old = prev == null ? null : members.get(prev);
            Set<Long> draining = new HashSet<>();         // a live old owner finishes and commits what it is fetching
            if (old != null) {
                old.owned.remove(p);
                for (Entry e : old.inFlight.values()) if (e.partition == p) draining.add(e.offset);
            }
            epoch[p]++;                                   // anything delivered before this handover is now stale
            if (next == null) { ownerNode.remove(p); continue; }
            ownerNode.put(p, next);
            Member m = members.get(next);
            m.owned.add(p);
            Instant now = clock.instant();
            for (CrawlTask t : log.get(p).values()) {
                Instant nb = hostNotBefore.get(t.host());
                if (nb != null && nb.isAfter(now) && !m.inFlight.containsKey(t.host())) m.queue.release(t.host(), nb);
            }
            for (Map.Entry<Long, CrawlTask> e : log.get(p).entrySet())
                if (!draining.contains(e.getKey())) m.deliver(new Entry(p, e.getKey(), epoch[p]), e.getValue());
        }
    }

    private static long fnv(String s) {
        long h = 0xcbf29ce484222325L;
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) { h ^= b; h *= 0x100000001b3L; }
        return h;
    }

    /** SplitMix64 finalizer: spreads similar inputs (node-1, node-2) across the whole range. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /** One node's view: its back queues over the partitions it owns. */
    private final class Member implements Frontier {
        final String nodeId;
        final InMemoryFrontier queue = new InMemoryFrontier(clock);
        final Set<Integer> owned = new HashSet<>();
        /** Delivered but not yet polled, by task instance (a replay of the same task replaces the entry). */
        final Map<CrawlTask, Entry> delivered = new IdentityHashMap<>();
        /** Polled, not yet released: at most one per host. */
        final Map<String, Entry> inFlight = new HashMap<>();
        volatile boolean closed;

        Member(String nodeId) { this.nodeId = nodeId; }

        void deliver(Entry e, CrawlTask t) {         // under the frontier monitor
            delivered.put(t, e);
            queue.push(t);
        }

        @Override public void push(CrawlTask task) { produce(task); }

        @Override public CrawlTask poll(long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
            while (!closed) {
                long left = (deadline - System.nanoTime()) / 1_000_000;
                CrawlTask t = queue.poll(Math.max(0, left));
                if (t == null) return null;
                synchronized (PartitionedFrontier.this) {
                    Entry e = delivered.remove(t);
                    if (e != null && claim(this, e)) {
                        inFlight.put(t.host(), e);
                        return t;
                    }
                }
                queue.release(t.host(), clock.instant());   // stale: moved away, replayed, or already committed
                if (left <= 0) return null;
            }
            return null;
        }

        @Override public void release(String host, Instant notBefore) {
            synchronized (PartitionedFrontier.this) {
                Entry e = inFlight.remove(host);
                if (e != null) commit(this, e, host, notBefore);
            }
            queue.release(host, notBefore);
        }

        @Override public long size() { return queue.size(); }

        @Override public String toString() { return "member " + nodeId; }
    }
}
