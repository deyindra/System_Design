package com.salesforce.einstein.webcrawler.url;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Thread-safe Bloom filter: "definitely not seen" or "probably seen".
 *
 * <p>Used as a <b>negative cache</b> in front of the global {@code url_records} table: most discovered URLs
 * were never crawled by anyone, and the filter answers that without a database round trip. It is never the
 * per-job seen-set, because a false positive there would silently drop a page from a customer's result.
 *
 * <p>Sizing: {@code m = -n·ln(p) / ln(2)²}, {@code k = m/n · ln 2}. 10 B URLs at 1% ≈ 96 Gbit ≈ 12 GB, k = 7.
 */
public final class BloomFilter {

    private final AtomicLongArray bits;
    private final long m;
    private final int k;

    public BloomFilter(long expectedItems, double falsePositiveRate) {
        long mm = (long) Math.ceil(-expectedItems * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
        this.m = Math.max(64, mm);
        this.k = Math.max(1, (int) Math.round((double) m / expectedItems * Math.log(2)));
        this.bits = new AtomicLongArray((int) ((m + 63) / 64));
    }

    public void put(String key) {
        long[] h = hashes(key);
        for (int i = 0; i < k; i++) {
            long bit = index(h, i);
            int word = (int) (bit >>> 6);
            long mask = 1L << (bit & 63);
            long cur;
            do { cur = bits.get(word); } while ((cur & mask) == 0 && !bits.compareAndSet(word, cur, cur | mask));
        }
    }

    public boolean mightContain(String key) {
        long[] h = hashes(key);
        for (int i = 0; i < k; i++) {
            long bit = index(h, i);
            if ((bits.get((int) (bit >>> 6)) & (1L << (bit & 63))) == 0) return false;
        }
        return true;
    }

    public int hashFunctions() { return k; }

    /** Kirsch–Mitzenmacher: k indexes from two 64-bit hashes. */
    private long index(long[] h, int i) { return Math.floorMod(h[0] + i * h[1], m); }

    private static long[] hashes(String key) {
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        long h1 = 0xcbf29ce484222325L, h2 = 0x84222325cbf29ce4L;
        for (byte x : b) {
            h1 = (h1 ^ x) * 0x100000001b3L;
            h2 = (h2 ^ x) * 0x9E3779B97F4A7C15L + 0x632BE59BD9B4E019L;
        }
        return new long[]{mix(h1), mix(h2) | 1};
    }

    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
