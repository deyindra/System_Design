package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.store.InMemoryJobStore;
import com.salesforce.einstein.webcrawler.store.JobStore;

class InMemoryJobStoreTest extends JobStoreContract {

    private final JobStore store = new InMemoryJobStore();

    @Override protected JobStore store() { return store; }
}
