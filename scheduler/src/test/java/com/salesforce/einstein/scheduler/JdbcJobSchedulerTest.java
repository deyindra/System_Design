package com.salesforce.einstein.scheduler;

import java.time.Duration;

/** Multi-VM mode with a cluster of one: the same behavior, through the shared JDBC store (H2). */
class JdbcJobSchedulerTest extends AbstractJobSchedulerTest {

    @Override
    protected JobScheduler create(int poolSize, int maxTasks, int maxConsecutiveTimeouts,
                                  String jobType, JobHandler handler) {
        JobScheduler.Builder b = JobScheduler.builder()
                .poolSize(poolSize).maxTasks(maxTasks).maxConsecutiveTimeouts(maxConsecutiveTimeouts)
                .clock(new SchedulerClock.SystemClock(UTC))
                .store(H2.store(H2.freshDatabase()))
                .pollInterval(Duration.ofMillis(25));
        if (jobType != null) b.registerJob(jobType, handler);
        return b.build();
    }
}
