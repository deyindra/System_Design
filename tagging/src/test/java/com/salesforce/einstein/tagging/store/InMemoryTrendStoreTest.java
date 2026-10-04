package com.salesforce.einstein.tagging.store;

import com.salesforce.einstein.tagging.spi.TrendStore;
import com.salesforce.einstein.tagging.spi.TrendStoreContractTest;

class InMemoryTrendStoreTest extends TrendStoreContractTest {
    @Override
    protected TrendStore newStore() {
        return new InMemoryTrendStore();
    }
}
