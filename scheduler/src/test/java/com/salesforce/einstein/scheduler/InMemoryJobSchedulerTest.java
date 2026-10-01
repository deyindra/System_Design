package com.salesforce.einstein.scheduler;

/**
 * Single-VM mode. Uses the original 4-argument constructor (in-memory store) wherever no job type is
 * needed, so the pre-SPI public API stays covered.
 */
class InMemoryJobSchedulerTest extends AbstractJobSchedulerTest {

    @Override
    protected JobScheduler create(int poolSize, int maxTasks, int maxConsecutiveTimeouts,
                                  String jobType, JobHandler handler) {
        if (jobType == null)
            return new JobScheduler(poolSize, maxTasks, maxConsecutiveTimeouts, new SchedulerClock.SystemClock(UTC));
        return JobScheduler.builder()
                .poolSize(poolSize).maxTasks(maxTasks).maxConsecutiveTimeouts(maxConsecutiveTimeouts)
                .clock(new SchedulerClock.SystemClock(UTC))
                .registerJob(jobType, handler)
                .build();
    }
}
