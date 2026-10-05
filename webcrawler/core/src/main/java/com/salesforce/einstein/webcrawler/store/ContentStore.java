package com.salesforce.einstein.webcrawler.store;

import java.util.Optional;

/**
 * Content-addressed blobs. Keyed by SHA-256 of the bytes, so the same logo on a million pages, or the same page
 * under two URLs, is stored once, and writes are idempotent (safe to retry, no coordination between workers).
 *
 * <p>This is a port: any object store with put/get/exists by key satisfies it (local disk, NFS, or a cloud bucket
 * through an adapter module). It needs no conditional writes, listing, or vendor features, because the key is
 * the content. Every adapter uses the {@link #key} layout, so blobs written by one implementation can be read by
 * another after a copy.
 */
public interface ContentStore {

    /** @return {@code true} if the blob was new */
    boolean put(String contentHash, byte[] bytes);

    Optional<byte[]> get(String contentHash);

    boolean contains(String contentHash);

    /** {@code ab/abcdef…}: a two-character prefix spreads keys across directories and object-store partitions. */
    static String key(String contentHash) {
        if (contentHash.length() < 3 || !contentHash.chars().allMatch(c -> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            throw new IllegalArgumentException("not a lowercase hex content hash");
        return contentHash.substring(0, 2) + "/" + contentHash;
    }
}
