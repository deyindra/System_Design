package com.salesforce.einstein.webcrawler.store;

import com.salesforce.einstein.webcrawler.model.PageSnapshot;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryPageStore implements PageStore {

    private final Map<String, PageSnapshot> latest = new ConcurrentHashMap<>();

    @Override public Optional<PageSnapshot> get(String urlHash) { return Optional.ofNullable(latest.get(urlHash)); }

    /** Last writer wins by {@code fetchedAt}, so a slow, older fetch never overwrites a newer one. */
    @Override public void put(PageSnapshot s) {
        latest.merge(s.urlHash(), s, (old, neu) -> neu.fetchedAt().isBefore(old.fetchedAt()) ? old : neu);
    }
}
