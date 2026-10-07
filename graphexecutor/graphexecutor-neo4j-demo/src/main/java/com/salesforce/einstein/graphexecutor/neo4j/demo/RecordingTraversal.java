package com.salesforce.einstein.graphexecutor.neo4j.demo;

import com.salesforce.einstein.graphexecutor.distributed.DistributedTraversalExecutor;
import com.salesforce.einstein.graphexecutor.distributed.Partitioner;
import com.salesforce.einstein.graphexecutor.distributed.Worker;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jWorkerContext;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * {@code WORKER_FACTORY} for a breadth-first traversal of a string-keyed graph whose task is {@link TaskRuns}.
 * {@code ROOTS} (comma-separated keys) are where it starts, or every source when unset; {@code TASK_TIME}
 * (ISO-8601, default PT0S) is how long each task takes.
 */
public final class RecordingTraversal implements DemoWorkerMain.Factory {

    /** The executor: hooks only (in cluster mode its pool and partitions are not used). */
    public static final class Executor extends DistributedTraversalExecutor<String> {
        private final TaskRuns runs;

        /** @param runs null: tasks do nothing */
        public Executor(TaskRuns runs) {
            super(ForkJoinPool.commonPool(), 4, Partitioner.hash());
            this.runs = runs;
        }

        @Override
        protected void executeTask(String node, int depth) throws InterruptedException {
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
        Set<String> roots = Arrays.stream(context.env("ROOTS", "").split(","))
                .map(String::trim).filter(root -> !root.isEmpty()).collect(Collectors.toSet());
        return Worker.traversal(new Executor(runs), roots, context.graphStore(codec), context.completionLog(codec),
                codec, context.cluster(), context.config());
    }
}
