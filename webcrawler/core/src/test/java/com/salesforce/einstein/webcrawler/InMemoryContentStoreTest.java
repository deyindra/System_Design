package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.store.InMemoryContentStore;

class InMemoryContentStoreTest extends ContentStoreContract {
    @Override ContentStore store() { return new InMemoryContentStore(); }
}
