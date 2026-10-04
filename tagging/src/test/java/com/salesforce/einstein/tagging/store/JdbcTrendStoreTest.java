package com.salesforce.einstein.tagging.store;

import com.salesforce.einstein.tagging.spi.TrendStore;
import com.salesforce.einstein.tagging.spi.TrendStoreContractTest;

import java.time.Clock;

class JdbcTrendStoreTest extends TrendStoreContractTest {
    @Override
    protected TrendStore newStore() {
        return new JdbcTrendStore(H2.newTrendDatabase(), Clock.systemUTC());
    }
}
