package com.salesforce.einstein.webcrawler.fetch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * DNS in front of the fetcher. A cold lookup costs 50–200 ms and public resolvers rate-limit a crawler doing
 * 10k lookups/s, so every worker caches answers (and negative answers). In production this is a sharded resolver
 * tier (unbound/CoreDNS) keyed by host, so each cache stays warm, and the frontier prefetches hosts it is about to
 * hand out; the IP is also what per-IP politeness is computed on.
 */
public final class CachingDnsResolver {

    private record Entry(InetAddress[] addresses, UnknownHostException error, Instant expiresAt) { }

    private final Function<String, InetAddress[]> lookup;
    private final Clock clock;
    private final Duration ttl;
    private final Duration negativeTtl;
    private final Map<String, Entry> cache = new ConcurrentHashMap<>();

    public CachingDnsResolver(Function<String, InetAddress[]> lookup, Clock clock, Duration ttl, Duration negativeTtl) {
        this.lookup = lookup;
        this.clock = clock;
        this.ttl = ttl;
        this.negativeTtl = negativeTtl;
    }

    public static CachingDnsResolver system(Clock clock) {
        return new CachingDnsResolver(h -> {
            try { return InetAddress.getAllByName(h); }
            catch (UnknownHostException e) { throw new IllegalStateException(e); }
        }, clock, Duration.ofMinutes(5), Duration.ofMinutes(1));
    }

    public InetAddress[] resolve(String host) throws UnknownHostException {
        Instant now = clock.instant();
        Entry e = cache.get(host);
        if (e == null || !now.isBefore(e.expiresAt)) {
            try {
                e = new Entry(lookup.apply(host), null, now.plus(ttl));
            } catch (RuntimeException ex) {
                e = new Entry(null, new UnknownHostException(host), now.plus(negativeTtl));
            }
            cache.put(host, e);
        }
        if (e.error != null) throw e.error;
        return e.addresses;
    }
}
