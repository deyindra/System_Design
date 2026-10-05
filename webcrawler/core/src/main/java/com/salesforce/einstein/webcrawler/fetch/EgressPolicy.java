package com.salesforce.einstein.webcrawler.fetch;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Which destinations the crawler may contact (SSRF guard). Seeds, links, redirects, robots.txt and webhook callbacks
 * are all attacker-controlled URLs, so without this a tenant could crawl {@code http://169.254.169.254/} (cloud
 * metadata credentials), {@code http://localhost:8080/actuator} or anything on the internal network.
 *
 * <p>Allowed: {@code http}/{@code https}, an allowed port, and a host that resolves <b>only</b> to public addresses.
 * Refused: loopback, unspecified ({@code 0.0.0.0/8}, {@code ::}), link-local (which includes the metadata address
 * {@code 169.254.169.254} on AWS, GCP and Azure), private ({@code 10/8}, {@code 172.16/12}, {@code 192.168/16}),
 * shared CGNAT ({@code 100.64/10}), unique-local IPv6 ({@code fc00::/7}, which includes AWS's {@code fd00:ec2::254}),
 * multicast, benchmarking and reserved ranges, and NAT64 addresses that embed any of those.
 *
 * <p>The check resolves through {@link CachingDnsResolver}, but the HTTP client resolves again when it connects, so
 * DNS rebinding can still slip between the two. Production closes that gap below the application: fetchers egress
 * only through a NAT whose network policy has no route to internal ranges, or through an egress proxy that pins the
 * checked address.
 */
public final class EgressPolicy {

    private final CachingDnsResolver dns;
    private final boolean allowPrivate;
    private final Set<Integer> ports;

    /** @param ports allowed destination ports; empty means any */
    public EgressPolicy(CachingDnsResolver dns, boolean allowPrivate, Set<Integer> ports) {
        this.dns = dns;
        this.allowPrivate = allowPrivate;
        this.ports = Set.copyOf(ports);
    }

    /** @return why {@code uri} must not be contacted, or empty if it may be */
    public Optional<String> check(URI uri) throws UnknownHostException {
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) return Optional.of("scheme not allowed: " + scheme);
        int port = uri.getPort() != -1 ? uri.getPort() : scheme.equals("https") ? 443 : 80;
        if (!ports.isEmpty() && !ports.contains(port)) return Optional.of("port not allowed: " + port);
        String host = uri.getHost();
        if (host == null || host.isEmpty()) return Optional.of("no host");
        if (allowPrivate) return Optional.empty();
        for (InetAddress a : dns.resolve(host))
            if (!isPublic(a)) return Optional.of(host + " resolves to non-public address " + a.getHostAddress());
        return Optional.empty();
    }

    static boolean isPublic(InetAddress a) {
        if (a.isLoopbackAddress() || a.isAnyLocalAddress() || a.isLinkLocalAddress() || a.isSiteLocalAddress()
                || a.isMulticastAddress()) return false;
        byte[] b = a.getAddress();
        if (b.length == 4) return isPublicV4(b);
        if ((b[0] & 0xfe) == 0xfc) return false;                                          // fc00::/7 unique local
        if (b[0] == 0 && b[1] == 0x64 && (b[2] & 0xff) == 0xff && (b[3] & 0xff) == 0x9b) { // 64:ff9b::/96 NAT64
            byte[] v4 = Arrays.copyOfRange(b, 12, 16);                                   // the embedded IPv4
            return isPublicV4(v4) && !isSpecialV4(v4);
        }
        return true;                                       // ::ffff:a.b.c.d arrives here already as an Inet4Address
    }

    private static boolean isPublicV4(byte[] v4) {
        int b0 = v4[0] & 0xff, b1 = v4[1] & 0xff, b2 = v4[2] & 0xff;
        if (b0 == 0 || b0 >= 240) return false;                                           // this network; reserved
        if (b0 == 100 && (b1 & 0xc0) == 64) return false;                                 // 100.64/10 shared CGNAT
        if (b0 == 198 && (b1 == 18 || b1 == 19)) return false;                            // 198.18/15 benchmarking
        return b0 != 192 || b1 != 0 || b2 != 0;                                // 192.0.0/24 IETF
    }

    /** Ranges {@link InetAddress} only recognizes as IPv4 objects, checked again when embedded in NAT64. */
    private static boolean isSpecialV4(byte[] v4) {
        int b0 = v4[0] & 0xff, b1 = v4[1] & 0xff;
        return b0 == 127 || b0 == 10 || (b0 == 172 && (b1 & 0xf0) == 16) || (b0 == 192 && b1 == 168)
                || (b0 == 169 && b1 == 254) || b0 >= 224;
    }
}
