package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.url.Hashing;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The behaviour every {@link ContentStore} adapter must have, whatever backs it. A provider module adds one
 * subclass that builds its store (against an emulator or a test bucket) and inherits every check.
 */
abstract class ContentStoreContract {

    abstract ContentStore store();

    static final byte[] BYTES = "<html>hello</html>".getBytes(StandardCharsets.UTF_8);
    static final String HASH = Hashing.sha256Hex(BYTES);

    @Test void putThenGetRoundTrips() {
        ContentStore s = store();
        assertFalse(s.contains(HASH));
        assertTrue(s.get(HASH).isEmpty());
        assertTrue(s.put(HASH, BYTES));
        assertTrue(s.contains(HASH));
        assertArrayEquals(BYTES, s.get(HASH).orElseThrow());
    }

    @Test void putIsIdempotent() {
        ContentStore s = store();
        assertTrue(s.put(HASH, BYTES));
        assertFalse(s.put(HASH, BYTES));
        assertArrayEquals(BYTES, s.get(HASH).orElseThrow());
    }

    @Test void callersCannotMutateStoredBytes() {
        ContentStore s = store();
        byte[] mine = BYTES.clone();
        s.put(HASH, mine);
        mine[0] = 'X';
        s.get(HASH).orElseThrow()[1] = 'Y';
        assertArrayEquals(BYTES, s.get(HASH).orElseThrow());
    }

    @Test void concurrentWritersOfOneHashAllSucceedAndExactlyOneIsNew() throws Exception {
        ContentStore s = store();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Boolean>> f = new ArrayList<>();
            for (int i = 0; i < 16; i++) f.add(pool.submit(() -> s.put(HASH, BYTES)));
            int fresh = 0;
            for (Future<Boolean> x : f) if (x.get()) fresh++;
            assertTrue(fresh >= 1);
            assertArrayEquals(BYTES, s.get(HASH).orElseThrow());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void keyLayoutIsSharedByEveryAdapter() {
        assertEquals("ab/abc123", ContentStore.key("abc123"));
        assertThrows(IllegalArgumentException.class, () -> ContentStore.key("../../etc/passwd"));
    }
}
