package com.salesforce.einstein.hierarchy.config;

import com.salesforce.einstein.hierarchy.events.OutboxRelay;
import com.salesforce.einstein.hierarchy.service.MoveJobWorker;
import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import org.springframework.scheduling.annotation.Scheduled;

/** The periodic tasks. Kept here so the components themselves don't depend on Spring scheduling. */
public class Schedules {
    private final OutboxRelay relay;
    private final MoveJobWorker worker;
    private final ShardRouter shards;
    private final HierarchyProperties props;

    Schedules(OutboxRelay relay, MoveJobWorker worker, ShardRouter shards, HierarchyProperties props) {
        this.relay = relay;
        this.worker = worker;
        this.shards = shards;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${hierarchy.relay.interval-ms:50}")
    public void relay() {
        if (props.relay().enabled()) {
            relay.relayAll();
        }
    }

    @Scheduled(fixedDelayString = "${hierarchy.relay.prune-interval-ms:3600000}", initialDelay = 60_000)
    public void prune() {
        if (props.relay().enabled()) {
            relay.prune();
        }
    }

    /** Only kicks the worker's own thread: a long move never holds up the scheduler. */
    @Scheduled(fixedDelayString = "${hierarchy.move.sweep-interval-ms:1000}")
    public void sweepMoveJobs() {
        worker.sweep();
    }

    @Scheduled(fixedDelayString = "${hierarchy.replicas.probe-interval-ms:20}")
    public void probeReplicas() {
        for (Shard s : shards.all()) {
            s.replicas().refresh();
        }
    }
}
