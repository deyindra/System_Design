package com.salesforce.einstein.hierarchy.spi;

import com.salesforce.einstein.hierarchy.domain.TreeEvent;

import java.util.List;

/** An in-process consumer of relayed events (cache invalidation, kicking the move worker). Must be idempotent. */
@FunctionalInterface
public interface TreeEventListener {
    void onEvents(List<TreeEvent> events);
}
