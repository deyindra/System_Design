package com.salesforce.einstein.webcrawler.store;

import com.salesforce.einstein.webcrawler.model.PageSnapshot;

import java.util.Optional;

/** Latest global fetch per URL ({@code url_records}; any key-value store keyed by {@code url_hash}, newest write wins). */
public interface PageStore {

    Optional<PageSnapshot> get(String urlHash);

    void put(PageSnapshot snapshot);
}
