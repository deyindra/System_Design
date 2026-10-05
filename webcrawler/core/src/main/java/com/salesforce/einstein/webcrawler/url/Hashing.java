package com.salesforce.einstein.webcrawler.url;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class Hashing {
    private Hashing() { }

    /** SHA-256 of the bytes, hex. Content hashes are the blob keys, so they must be collision-resistant. */
    public static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 128-bit URL id. */
    public static String urlHash(String canonicalUrl) {
        return sha256Hex(canonicalUrl.getBytes(StandardCharsets.UTF_8)).substring(0, 32);
    }
}
