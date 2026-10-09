package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

/**
 * Base-36 fractional ranks (the scheme Jira uses for issue order). A sibling's position is any string strictly between
 * its neighbours, so inserting or reordering touches one row. The result never ends in '0', so there is always room
 * between two ranks.
 *
 * <p>Appending (and prepending) is the common case, so an open end doesn't take the midpoint, which would add a digit
 * every few appends. It steps the neighbour by one at {@link #STEP_LENGTH} digits instead: {@code i -> i00001 ->
 * i00002}, about 60 million appends before the key grows.
 */
public final class LexoRank {
    private static final String DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int BASE = DIGITS.length();
    /** Above this length the parent's children are worth rebalancing; still correct, just longer keys. */
    public static final int REBALANCE_LENGTH = 32;
    static final int STEP_LENGTH = 6;

    private LexoRank() {
    }

    /** A rank strictly between {@code lo} and {@code hi}. Null means an open end. Requires {@code lo < hi}. */
    public static String between(@Nullable String lo, @Nullable String hi) {
        if (lo != null) {
            requireValid(lo);
        }
        if (hi != null) {
            requireValid(hi);
        }
        if (lo != null && hi != null && lo.compareTo(hi) >= 0) {
            throw new IllegalArgumentException("lo must sort before hi: " + lo + " >= " + hi);
        }
        if (lo != null && hi == null) {
            String next = step(lo, 1);
            if (next != null) {
                return next;
            }
        } else if (lo == null && hi != null) {
            String prev = step(hi, -1);
            if (prev != null) {
                return prev;
            }
        }
        String upper = hi;
        StringBuilder out = new StringBuilder();
        for (int i = 0; ; i++) {
            int a = lo != null && i < lo.length() ? DIGITS.indexOf(lo.charAt(i)) : 0;
            int b = upper != null && i < upper.length() ? DIGITS.indexOf(upper.charAt(i)) : BASE;
            if (a == b) {
                out.append(DIGITS.charAt(a));               // shared prefix
                continue;
            }
            int mid = (a + b) / 2;
            if (mid > a) {
                return out.append(DIGITS.charAt(mid)).toString();
            }
            out.append(DIGITS.charAt(a));                   // adjacent digits: go one level deeper,
            upper = null;                                   // where the upper bound is open
        }
    }

    /**
     * {@code rank} right-padded with '0' to {@link #STEP_LENGTH} digits, plus or minus one in base 36, skipping values
     * that end in '0'. Padding doesn't change the order, so the result is strictly after (or before) {@code rank}.
     * Null when the digits run out ({@code zzzzzz} + 1); the caller then goes one level deeper.
     */
    @Nullable
    private static String step(String rank, int delta) {
        char[] d = (rank.length() >= STEP_LENGTH ? rank : rank + "0".repeat(STEP_LENGTH - rank.length()))
                .toCharArray();
        do {
            int i = d.length - 1;
            while (true) {
                if (i < 0) {
                    return null;
                }
                int v = DIGITS.indexOf(d[i]) + delta;
                if (v >= 0 && v < BASE) {
                    d[i] = DIGITS.charAt(v);
                    break;
                }
                d[i] = delta > 0 ? DIGITS.charAt(0) : DIGITS.charAt(BASE - 1);   // carry / borrow
                i--;
            }
        } while (d[d.length - 1] == '0');
        return new String(d);
    }

    public static boolean isValid(String rank) {
        if (rank.isEmpty() || rank.charAt(rank.length() - 1) == '0') {
            return false;
        }
        for (int i = 0; i < rank.length(); i++) {
            if (DIGITS.indexOf(rank.charAt(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    private static void requireValid(String rank) {
        if (!isValid(rank)) {
            throw new IllegalArgumentException("not a rank: '" + rank + "'");
        }
    }
}
