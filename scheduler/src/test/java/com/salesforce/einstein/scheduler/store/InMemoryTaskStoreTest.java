package com.salesforce.einstein.scheduler.store;

import com.salesforce.einstein.scheduler.spi.TaskStore;
import com.salesforce.einstein.scheduler.spi.TaskStoreContractTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryTaskStoreTest extends TaskStoreContractTest {
    @Override protected TaskStore newStore() { return new InMemoryTaskStore(); }

    @Test
    void notSharedAndSingleOwner() {
        assertFalse(store.isShared());
        store.addChangeListener(() -> {});
        assertThrows(IllegalStateException.class, () -> store.addChangeListener(() -> {}),
                "one in-memory store serves exactly one scheduler");
    }
}
