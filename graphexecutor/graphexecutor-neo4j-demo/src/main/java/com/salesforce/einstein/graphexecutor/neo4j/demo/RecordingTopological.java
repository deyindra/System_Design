package com.salesforce.einstein.graphexecutor.neo4j.demo;

import com.salesforce.einstein.graphexecutor.distributed.DistributedTopologicalExecutor;
import com.salesforce.einstein.graphexecutor.distributed.Partitioner;
import com.salesforce.einstein.graphexecutor.distributed.Worker;
import com.salesforce.einstein.graphexecutor.executor.IneligibleGroupPolicy;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jWorkerContext;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.time.Duration;
import java.util.concurrent.ForkJoinPool;

/**
 * {@code WORKER_FACTORY} for a dependency-ordered run of a string-keyed graph whose task is {@link TaskRuns}:
 * a node runs once every predecessor has. {@code TASK_TIME} (ISO-8601, default PT0S) is how long each task takes.
 */
public final class RecordingTopological implements DemoWorkerMain.Factory {

    /** The executor: hooks only (in cluster mode its pool and partitions are not used). */
    public static final class Executor extends DistributedTopologicalExecutor<String> {
        private final TaskRuns runs;

        /** @param runs null: tasks do nothing */
        public Executor(TaskRuns runs) {
            super(ForkJoinPool.commonPool(), 4, Partitioner.hash(), IneligibleGroupPolicy.REJECT_GRAPH);
            this.runs = runs;
        }

        @Override
        protected void executeTask(String node) throws InterruptedException {
            if (runs != null) {
                runs.run(node);
            }
        }
    }

    @Override
    public Worker<String> create(Neo4jWorkerContext context) {
        NodeCodec<String> codec = NodeCodec.strings();
        TaskRuns runs = new TaskRuns(context.sessions(), context.config().runId(),
                context.config().workerId(), Duration.parse(context.env("TASK_TIME", "PT0S")));
        return Worker.topological(new Executor(runs), context.graphStore(codec), context.completionLog(codec), codec,
                context.cluster(), context.config());
    }
}
