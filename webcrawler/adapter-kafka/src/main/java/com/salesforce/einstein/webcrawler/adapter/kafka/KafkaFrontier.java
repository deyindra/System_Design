package com.salesforce.einstein.webcrawler.adapter.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.salesforce.einstein.webcrawler.frontier.Frontier;
import com.salesforce.einstein.webcrawler.frontier.HostSchedule;
import com.salesforce.einstein.webcrawler.frontier.InMemoryFrontier;
import com.salesforce.einstein.webcrawler.model.CrawlTask;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The {@link Frontier} over a Kafka-API log: one topic keyed by host, so all of a host's tasks land in one partition
 * and the consumer group gives each partition (and so each host) to exactly one node. That node feeds the records into
 * an {@link InMemoryFrontier}, its back queues, which apply politeness and BFS order exactly as on a single node.
 *
 * <p>Delivery is at least once. A record's offset is committed only after the worker released it, and the committed
 * offset of a partition is its lowest offset still being worked on, so a node that dies loses nothing: the next owner
 * replays from there, and the job store's task keys drop the copies of work that did finish.
 *
 * <p>Threads: one consumer thread owns the {@link KafkaConsumer} (it is not thread-safe); workers call
 * {@link #poll}/{@link #release} and hand finished offsets to it through a queue.
 */
public final class KafkaFrontier implements Frontier, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaFrontier.class);

    /**
     * One assigned partition, from assignment until revocation. A new assignment of the same partition is a new
     * object, so a task buffered under an old one is recognisably stale. Touched only by the consumer thread.
     */
    private static final class Owned {
        final TreeSet<Long> outstanding = new TreeSet<>();   // buffered or in flight, not yet released
        final Set<String> hosts = new HashSet<>();
        long next = -1;                                      // one past the last offset buffered
    }

    private record Delivery(TopicPartition tp, long offset, Owned owner) { }

    private final KafkaFrontierSettings settings;
    private final HostSchedule schedule;
    private final Clock clock;
    private final ObjectMapper json = JsonMapper.builder().findAndAddModules()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    private final InMemoryFrontier local;
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread consumerThread;

    private final Map<TopicPartition, Owned> owned = new ConcurrentHashMap<>();
    private final Map<CrawlTask, Delivery> delivered = Collections.synchronizedMap(new IdentityHashMap<>());
    private final Map<String, Delivery> inFlight = new ConcurrentHashMap<>();       // host → the task out for it
    private final Set<String> primed = ConcurrentHashMap.newKeySet();               // hosts whose delay was loaded
    private final ConcurrentLinkedQueue<Delivery> done = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<List<Future<RecordMetadata>>> unacked = ThreadLocal.withInitial(ArrayList::new);

    private volatile boolean running = true;
    private volatile boolean commitOnExit = true;
    private boolean dirty;                                   // consumer thread only
    private long lastCommit;                                 // consumer thread only

    public KafkaFrontier(KafkaFrontierSettings settings, HostSchedule schedule, Clock clock) {
        this.settings = settings;
        this.schedule = schedule;
        this.clock = clock;
        this.local = new InMemoryFrontier(clock);
        if (settings.createTopic()) createTopic();
        this.producer = new KafkaProducer<>(producerConfig(), new StringSerializer(), new ByteArraySerializer());
        this.consumer = new KafkaConsumer<>(consumerConfig(), new StringDeserializer(), new ByteArrayDeserializer());
        this.consumerThread = new Thread(this::consume, "frontier-consumer-" + settings.clientId());
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    // ------------------------------------------------------------------ producer side

    /** Asynchronous: the send is awaited by this thread's next {@link #release} or {@link #flush}. */
    @Override public void push(CrawlTask task) {
        byte[] value;
        try {
            value = json.writeValueAsBytes(task);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        unacked.get().add(producer.send(new ProducerRecord<>(settings.topic(), task.host(), value)));
    }

    @Override public void flush() {
        List<Future<RecordMetadata>> sends = unacked.get();
        try {
            for (Future<RecordMetadata> f : sends) f.get(settings.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the frontier log", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("a task could not be written to the frontier log", e);
        } finally {
            sends.clear();
        }
    }

    // ------------------------------------------------------------------ worker side

    @Override public CrawlTask poll(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            long left = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            CrawlTask t = local.poll(Math.max(0, left));
            if (t == null) return null;
            Delivery d = delivered.get(t);
            if (d == null || owned.get(d.tp()) != d.owner()) {    // its partition moved away: the new owner has it
                delivered.remove(t);
                local.release(t.host(), clock.instant());
                continue;
            }
            // First task for this host since we got its partition: the previous owner may have hit it a moment ago.
            if (primed.add(t.host())) {
                Optional<Instant> nb = schedule.notBefore(t.host()).filter(i -> i.isAfter(clock.instant()));
                if (nb.isPresent()) {
                    local.push(t);                                 // the host is ours (busy) here, so this is safe
                    local.release(t.host(), nb.get());
                    continue;
                }
            }
            delivered.remove(t);
            inFlight.put(t.host(), d);
            // A lease: if the partition moves mid-fetch, the next owner waits for this request to finish.
            schedule.put(t.host(), clock.instant().plus(settings.fetchLease()));
            return t;
        }
    }

    /**
     * Frees the host locally first, so a log outage can't wedge it. The task is acknowledged only once this thread's
     * pushes (its children) are durable; if they aren't, it stays uncommitted and is redelivered.
     */
    @Override public void release(String host, Instant notBefore) {
        Delivery d = inFlight.remove(host);
        local.release(host, notBefore);
        schedule.put(host, notBefore);
        flush();
        if (d != null) done.add(d);
    }

    /** Tasks buffered on <i>this</i> node; the cluster-wide backlog is the group's consumer lag. */
    @Override public long size() { return local.size(); }

    /** Partitions this node owns now. */
    public Set<Integer> partitions() {
        Set<Integer> out = new HashSet<>();
        owned.keySet().forEach(tp -> out.add(tp.partition()));
        return out;
    }

    /** The partition a host's tasks go to: the producer's default partitioner on the key. */
    public int partitionOf(String host) {
        return Utils.toPositive(Utils.murmur2(host.getBytes(StandardCharsets.UTF_8))) % settings.partitions();
    }

    // ------------------------------------------------------------------ consumer thread

    private void consume() {
        try {
            consumer.subscribe(List.of(settings.topic()), new Rebalance());
            while (running) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(100))) buffer(r);
                drainDone();
                if (dirty && clock.millis() - lastCommit >= settings.commitInterval().toMillis()) {
                    consumer.commitAsync(committable(owned.keySet()), (offsets, e) -> {
                        if (e != null) log.warn("frontier commit failed (retried on the next one): {}", e.toString());
                    });
                    dirty = false;
                    lastCommit = clock.millis();
                }
                // Back-pressure: stop fetching while this node has enough buffered; the rest waits in the log.
                if (local.size() >= settings.maxBuffered()) consumer.pause(consumer.assignment());
                else if (!consumer.paused().isEmpty()) consumer.resume(consumer.paused());
            }
        } catch (WakeupException e) {
            if (running) throw e;
        } catch (RuntimeException e) {
            log.error("frontier consumer stopped", e);
        } finally {
            try {
                if (commitOnExit) {
                    drainDone();
                    Map<TopicPartition, OffsetAndMetadata> offsets = committable(owned.keySet());
                    if (!offsets.isEmpty()) consumer.commitSync(offsets);
                }
            } catch (RuntimeException e) {
                log.warn("final frontier commit failed: {}", e.toString());
            }
            consumer.close(commitOnExit ? Duration.ofSeconds(5) : Duration.ZERO);
        }
    }

    private void buffer(ConsumerRecord<String, byte[]> r) {
        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
        Owned o = owned.get(tp);
        if (o == null) return;                                     // revoked inside this poll
        o.next = r.offset() + 1;
        dirty = true;
        CrawlTask t;
        try {
            t = json.readValue(r.value(), CrawlTask.class);
        } catch (IOException e) {
            log.error("dropping an unreadable frontier record {}@{}: {}", tp, r.offset(), e.toString());
            return;                                                // not outstanding, so it is committed past
        }
        o.outstanding.add(r.offset());
        o.hosts.add(t.host());
        delivered.put(t, new Delivery(tp, r.offset(), o));         // before push: a worker may take it at once
        local.push(t);
    }

    private void drainDone() {
        for (Delivery d; (d = done.poll()) != null; ) {
            if (owned.get(d.tp()) != d.owner()) continue;          // released after its partition left: the new
            d.owner().outstanding.remove(d.offset());              // owner replays it, task keys drop the copy
            dirty = true;
        }
    }

    /** Lowest offset still outstanding per partition, or one past the last buffered: everything below is done. */
    private Map<TopicPartition, OffsetAndMetadata> committable(Collection<TopicPartition> tps) {
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
        for (TopicPartition tp : tps) {
            Owned o = owned.get(tp);
            if (o == null || o.next < 0) continue;
            out.put(tp, new OffsetAndMetadata(o.outstanding.isEmpty() ? o.next : o.outstanding.first()));
        }
        return out;
    }

    private void retire(Collection<TopicPartition> tps) {
        for (TopicPartition tp : tps) {
            Owned o = owned.remove(tp);
            if (o != null) primed.removeAll(o.hosts);              // if it comes back, reload the delays
        }
    }

    private final class Rebalance implements ConsumerRebalanceListener {
        @Override public void onPartitionsRevoked(Collection<TopicPartition> tps) {
            if (commitOnExit) {
                drainDone();
                Map<TopicPartition, OffsetAndMetadata> offsets = committable(tps);
                try {
                    if (!offsets.isEmpty()) consumer.commitSync(offsets);
                } catch (RuntimeException e) {
                    log.warn("commit on revoke failed; the new owner replays a little more: {}", e.toString());
                }
            }
            retire(tps);
            log.info("{} gave up partitions {}", settings.clientId(), tps);
        }

        @Override public void onPartitionsLost(Collection<TopicPartition> tps) { retire(tps); }

        @Override public void onPartitionsAssigned(Collection<TopicPartition> tps) {
            for (TopicPartition tp : tps) owned.put(tp, new Owned());
            log.info("{} took partitions {}", settings.clientId(), tps);
        }
    }

    // ------------------------------------------------------------------ lifecycle

    /** Graceful: acknowledges what was released, leaves the group, closes the producer. */
    @Override public void close() {
        stopConsumer();
        producer.close(Duration.ofSeconds(5));
    }

    /**
     * Like a node dying: leaves the group without committing, so its partitions and their unfinished tasks go to
     * the survivors. The producer stays open, as a paused process's would (a zombie may still push).
     */
    public void kill() {
        commitOnExit = false;
        stopConsumer();
    }

    private void stopConsumer() {
        if (!running) return;
        running = false;
        consumer.wakeup();
        try {
            consumerThread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ config

    private void createTopic() {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(settings.topic(), settings.partitions(), settings.replicationFactor())))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("could not create topic " + settings.topic(), e);
        }
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> c = new HashMap<>(settings.extra());
        c.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        c.put(ProducerConfig.CLIENT_ID_CONFIG, settings.clientId() + "-producer");
        c.put(ProducerConfig.ACKS_CONFIG, "all");
        c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        c.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return c;
    }

    private Map<String, Object> consumerConfig() {
        Map<String, Object> c = new HashMap<>(settings.extra());
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, settings.clientId());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Sticky + cooperative: a node joining or leaving moves only the partitions it must, and the others keep
        // fetching through the rebalance (eager rebalancing would stop the whole fleet).
        c.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        c.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) settings.sessionTimeout().toMillis());
        c.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, (int) Math.max(500, settings.sessionTimeout().toMillis() / 4));
        return c;
    }
}
