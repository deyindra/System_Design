package com.salesforce.einstein.hierarchy.domain;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LexoRankTest {
    @Test
    void firstRankIsTheMiddle() {
        assertThat(LexoRank.between(null, null)).isEqualTo("i");
    }

    @Test
    void openEndsStepByOneAtFixedWidth() {
        assertThat(LexoRank.between("i", null)).isEqualTo("i00001");
        assertThat(LexoRank.between("i00009", null)).isEqualTo("i0000a");
        assertThat(LexoRank.between("i0000z", null)).isEqualTo("i00011");   // i00010 ends in 0: skipped
        assertThat(LexoRank.between(null, "i")).isEqualTo("hzzzzz");
        assertThat(LexoRank.between(null, "i00011")).isEqualTo("i0000z");
    }

    @Test
    void openEndsGoDeeperWhenTheDigitsRunOut() {
        assertThat(LexoRank.between("zzzzzz", null)).isEqualTo("zzzzzzi");
        assertThat(LexoRank.between(null, "000001")).isEqualTo("000000i");
    }

    @Test
    void adjacentDigitsGoOneLevelDeeper() {
        assertThat(LexoRank.between("a", "b")).isEqualTo("ai");
        assertThat(LexoRank.between("a", "a1")).isEqualTo("a0i");
        assertThat(LexoRank.between("az", "b")).isEqualTo("azi");
    }

    @Test
    void rejectsBadInput() {
        assertThatThrownBy(() -> LexoRank.between("b", "a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LexoRank.between("a", "a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LexoRank.between("a0", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LexoRank.between("A", null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isValid() {
        assertThat(LexoRank.isValid("i")).isTrue();
        assertThat(LexoRank.isValid("0i")).isTrue();
        assertThat(LexoRank.isValid("")).isFalse();
        assertThat(LexoRank.isValid("i0")).isFalse();
        assertThat(LexoRank.isValid("i-")).isFalse();
    }

    /** Random inserts anywhere in a list: every new rank sorts strictly between its neighbours. */
    @Test
    void fuzzRandomInsertsKeepTotalOrder() {
        Random rnd = new Random(42);
        List<String> ranks = new ArrayList<>();
        for (int n = 0; n < 20_000; n++) {
            int at = rnd.nextInt(ranks.size() + 1);
            String lo = at == 0 ? null : ranks.get(at - 1);
            String hi = at == ranks.size() ? null : ranks.get(at);
            String r = LexoRank.between(lo, hi);
            assertThat(LexoRank.isValid(r)).isTrue();
            if (lo != null) {
                assertThat(r).isGreaterThan(lo);
            }
            if (hi != null) {
                assertThat(r).isLessThan(hi);
            }
            ranks.add(at, r);
        }
        assertThat(ranks).isSorted();
    }

    /** The worst case: always inserting at the same spot only grows the key by about one char per 5 inserts. */
    @Test
    void repeatedInsertsAtOneSpotGrowSlowly() {
        String lo = "i";
        String hi = "j";
        for (int n = 0; n < 100; n++) {
            hi = LexoRank.between(lo, hi);
        }
        assertThat(hi.length()).isLessThanOrEqualTo(LexoRank.REBALANCE_LENGTH);
    }

    @Test
    void appendsAndPrependsStayShort() {
        String last = null;
        String first = null;
        for (int n = 0; n < 100_000; n++) {
            String next = LexoRank.between(last, null);
            assertThat(last == null || next.compareTo(last) > 0).isTrue();
            last = next;
            String prev = LexoRank.between(null, first == null ? "i" : first);
            assertThat(first == null || prev.compareTo(first) < 0).isTrue();
            first = prev;
        }
        assertThat(last).hasSize(LexoRank.STEP_LENGTH);
        assertThat(first).hasSize(LexoRank.STEP_LENGTH);
    }
}
