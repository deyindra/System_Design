package com.salesforce.einstein.webcrawler.frontier;

import java.time.Instant;
import java.util.Optional;

/**
 * When each host may next be contacted, shared by every frontier node. A partition that moves to another node takes
 * its hosts with it, but not their politeness delays (those live in the old owner's back queues). The new owner reads
 * them here before its first request to each host, so a handover doesn't hit a host twice in quick succession.
 *
 * <p>Advisory: a lost write costs at most one early request, never a lost or duplicated task.
 */
public interface HostSchedule {

    Optional<Instant> notBefore(String host);

    /** Records the host's next allowed time; an entry may expire once that time has passed. */
    void put(String host, Instant notBefore);
}
