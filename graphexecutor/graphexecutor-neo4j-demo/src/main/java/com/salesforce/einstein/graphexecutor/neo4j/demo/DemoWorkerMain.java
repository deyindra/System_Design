package com.salesforce.einstein.graphexecutor.neo4j.demo;

import com.salesforce.einstein.graphexecutor.distributed.ClusterReport;
import com.salesforce.einstein.graphexecutor.distributed.Worker;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jWorkerContext;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/**
 * The demo image's entrypoint: a worker process that runs the {@link Factory} named by {@code WORKER_FACTORY}
 * ({@link RecordingTraversal} or {@link RecordingTopological}), beside the settings of {@link Neo4jWorkerContext}.
 * The factory has a public no-arg constructor. The process logs the run's report, then exits 0 when the run is
 * done, 1 when it failed; a crash (an exception) exits non-zero too, and the other workers take its shards over.
 */
public final class DemoWorkerMain {

    private static final Logger LOG = System.getLogger(DemoWorkerMain.class.getName());

    /** Makes this process's worker. */
    @FunctionalInterface
    public interface Factory {
        Worker<?> create(Neo4jWorkerContext context) throws Exception;
    }

    private DemoWorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        ClusterReport report;
        try (Neo4jWorkerContext context = Neo4jWorkerContext.of(System.getenv())) {
            Factory factory = Class.forName(Neo4jWorkerContext.require(context.env(), "WORKER_FACTORY"))
                    .asSubclass(Factory.class).getDeclaredConstructor().newInstance();
            report = factory.create(context).run();
        }
        LOG.log(Level.INFO, report);
        System.exit(report.status() == RunStatus.DONE ? 0 : 1);
    }
}
