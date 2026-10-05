package com.salesforce.einstein.webcrawler.store;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryContentStore implements ContentStore {

    private final Map<String, byte[]> blobs = new ConcurrentHashMap<>();

    @Override public boolean put(String contentHash, byte[] bytes) {
        return blobs.putIfAbsent(contentHash, bytes.clone()) == null;
    }

    @Override public Optional<byte[]> get(String contentHash) {
        return Optional.ofNullable(blobs.get(contentHash)).map(byte[]::clone);
    }

    @Override public boolean contains(String contentHash) { return blobs.containsKey(contentHash); }

    /** Not part of the port: counting is a full listing on an object store. Tests use it to check dedup. */
    public int size() { return blobs.size(); }
}
