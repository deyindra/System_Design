package com.salesforce.einstein.tagging.domain;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Opaque, versioned cursors. Clients must treat them as blobs, which lets the encoding change without
 * breaking callers. Two kinds exist: a numeric keyset ({@code entitySeq}) and a string keyset (tag name).
 */
public final class Cursors {
    private static final String SEQ = "s1:";
    private static final String NAME = "n1:";

    private Cursors() {
    }

    public static String ofSeq(long seq) {
        return encode(SEQ + seq);
    }

    public static long seq(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        String s = decode(cursor);
        if (!s.startsWith(SEQ)) {
            throw new InvalidRequestException("invalid cursor");
        }
        try {
            long v = Long.parseLong(s.substring(SEQ.length()));
            if (v < 0) {
                throw new InvalidRequestException("invalid cursor");
            }
            return v;
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("invalid cursor");
        }
    }

    public static String ofName(String nameNorm) {
        return encode(NAME + nameNorm);
    }

    public static String name(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String s = decode(cursor);
        if (!s.startsWith(NAME)) {
            throw new InvalidRequestException("invalid cursor");
        }
        return s.substring(NAME.length());
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
