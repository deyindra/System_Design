package com.salesforce.einstein.tagging.store;

import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TagStoreContractTest;

import java.time.Clock;
import java.time.ZoneOffset;

class InMemoryTagStoreTest extends TagStoreContractTest {
    @Override
    protected TagStore newStore() {
        return new InMemoryTagStore("s0", Clock.fixed(NOW, ZoneOffset.UTC));
    }
}
