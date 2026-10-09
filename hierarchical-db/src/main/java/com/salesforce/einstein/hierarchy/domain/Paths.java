package com.salesforce.einstein.hierarchy.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Materialized paths: the ids from the root down to the node itself, each followed by '/', e.g. {@code /100/104/101/}.
 *
 * <ul>
 *   <li>Every path ends with '/', so the prefix {@code /100/1/} never matches {@code /100/10/}.</li>
 *   <li>Under {@code COLLATE "C"} the rows that start with {@code p} are exactly {@code [p, upperBound(p))}: the last
 *       '/' (0x2F) becomes '0' (0x30), the next byte up. That is one B-tree range scan.</li>
 * </ul>
 */
public final class Paths {
    /** Depth is 0 for a root, so 64 levels. Bounded so a path stays far below the B-tree's 2.7 KB entry limit. */
    public static final int MAX_DEPTH = 63;
    private static final Pattern VALID = Pattern.compile("(/\\d{1,19})+/");

    private Paths() {
    }

    public static String root(long id) {
        return "/" + id + "/";
    }

    public static String child(String parentPath, long id) {
        return parentPath + id + "/";
    }

    /** The smallest string greater than every string that starts with {@code prefix}. */
    public static String upperBound(String prefix) {
        if (!prefix.endsWith("/")) {
            throw new IllegalArgumentException("not a path prefix: " + prefix);
        }
        return prefix.substring(0, prefix.length() - 1) + '0';
    }

    public static boolean isUnder(String path, String prefix) {
        return path.startsWith(prefix);
    }

    /** Root first, ending with the node itself. */
    public static List<Long> ids(String path) {
        List<Long> out = new ArrayList<>();
        int start = 1;
        for (int i = 1; i < path.length(); i++) {
            if (path.charAt(i) == '/') {
                out.add(Long.parseLong(path, start, i, 10));
                start = i + 1;
            }
        }
        return out;
    }

    public static boolean isValid(String path) {
        return VALID.matcher(path).matches();
    }
}
