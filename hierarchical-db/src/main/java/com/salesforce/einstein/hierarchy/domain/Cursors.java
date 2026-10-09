package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Opaque, versioned keyset cursors. Clients treat them as blobs, so the encoding can change without breaking them.
 * Children page on {@code (rank, node_id)}; subtrees page on the last effective path, which is pre-order.
 */
public final class Cursors {
    private static final String CHILD = "c1:";
    private static final String PATH = "p1:";

    private Cursors() {
    }

    /** The position after one child. */
    public record ChildKey(String rank, long nodeId) {
    }

    public static String ofChild(String rank, long nodeId) {
        return encode(CHILD + rank + ":" + nodeId);
    }

    @Nullable
    public static ChildKey child(@Nullable String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String s = decode(cursor);
        int sep = s.lastIndexOf(':');
        if (!s.startsWith(CHILD) || sep <= CHILD.length()) {
            throw new InvalidRequestException("invalid cursor");
        }
        String rank = s.substring(CHILD.length(), sep);
        if (!LexoRank.isValid(rank)) {
            throw new InvalidRequestException("invalid cursor");
        }
        try {
            return new ChildKey(rank, Long.parseLong(s.substring(sep + 1)));
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("invalid cursor");
        }
    }

    public static String ofPath(String path) {
        return encode(PATH + path);
    }

    @Nullable
    public static String path(@Nullable String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String s = decode(cursor);
        if (!s.startsWith(PATH) || !Paths.isValid(s.substring(PATH.length()))) {
            throw new InvalidRequestException("invalid cursor");
        }
        return s.substring(PATH.length());
    }

    private static String encode(String s) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String cursor) {
        try {
            return new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("invalid cursor");
        }
    }
}
