package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;

/** Running one task the way every partition does: the outcome is logged before anything is sent for it. */
final class Tasks {
    private Tasks() {
    }

    /** A task: user code, so it may throw anything. */
    @FunctionalInterface
    interface Task {
        void run() throws Exception;
    }

    /**
     * Runs {@code task} and logs {@code DONE}, or {@code FAILED} with what it threw. An interrupt is
     * restored for the caller to see.
     *
     * @return what the task threw, or null if it completed
     */
    static <T> Exception runAndLog(CompletionLog<T> log, T node, int round, Task task) {
        try {
            task.run();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.record(node, new Outcome(Status.FAILED, round, String.valueOf(e)));
            return e;
        }
        // a crash between the task and this line runs the task again on replay: why tasks must be idempotent
        log.record(node, new Outcome(Status.DONE, round, null));
        return null;
    }
}
