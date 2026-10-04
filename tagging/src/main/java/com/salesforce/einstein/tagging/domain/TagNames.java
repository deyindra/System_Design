package com.salesforce.einstein.tagging.domain;

import java.text.Normalizer;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Tag name rules. Two names that differ only in case, Unicode form or surrounding/inner whitespace
 * are the same tag ("Bug Bash", "bug  bash" and "ＢＵＧ bash" all map to {@code bug bash}).
 */
public final class TagNames {
    public static final int MAX_LENGTH = 64;
    private static final Pattern WS = Pattern.compile("\\s+");
    private static final String TOMBSTONE_PREFIX = "\u0001deleted:";

    private TagNames() {
    }

    /** The name as shown: NFKC, trimmed, inner whitespace collapsed, original case kept. */
    public static String display(String raw) {
        if (raw == null) {
            throw new InvalidRequestException("tag name is required");
        }
        String s = WS.matcher(Normalizer.normalize(raw, Normalizer.Form.NFKC).strip()).replaceAll(" ");
        if (s.isEmpty()) {
            throw new InvalidRequestException("tag name must not be blank");
        }
        if (s.codePointCount(0, s.length()) > MAX_LENGTH) {
            throw new InvalidRequestException("tag name longer than " + MAX_LENGTH + " characters");
        }
        for (int i = 0; i < s.length(); i++) {
            if (Character.isISOControl(s.charAt(i))) {
                throw new InvalidRequestException("tag name must not contain control characters");
            }
        }
        return s;
    }

    /** The uniqueness key: {@link #display} lower-cased with {@link Locale#ROOT}. */
    public static String normalize(String raw) {
        return display(raw).toLowerCase(Locale.ROOT);
    }

    /** Normalizes a search prefix without rejecting an empty one. */
    public static String normalizePrefix(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        return WS.matcher(Normalizer.normalize(raw, Normalizer.Form.NFKC).stripLeading()).replaceAll(" ")
                .toLowerCase(Locale.ROOT);
    }

    static String tombstone(long tagId) {
        return TOMBSTONE_PREFIX + tagId;
    }
}
