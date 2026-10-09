package com.salesforce.einstein.hierarchy.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PathsTest {
    @Test
    void buildsPaths() {
        assertThat(Paths.root(100)).isEqualTo("/100/");
        assertThat(Paths.child("/100/104/", 101)).isEqualTo("/100/104/101/");
    }

    @Test
    void upperBoundIsTheNextByteAfterTheTrailingSlash() {
        assertThat(Paths.upperBound("/100/104/")).isEqualTo("/100/1040");
        // Everything under the prefix sorts in [prefix, upper); a sibling with a longer id does not.
        String p = "/100/1/";
        String upper = Paths.upperBound(p);
        assertThat("/100/1/5/").isGreaterThanOrEqualTo(p).isLessThan(upper);
        assertThat("/100/10/").isGreaterThanOrEqualTo(upper);
        assertThatThrownBy(() -> Paths.upperBound("/100")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void prefixNeverMatchesALongerId() {
        assertThat(Paths.isUnder("/100/10/", "/100/1/")).isFalse();
        assertThat(Paths.isUnder("/100/1/7/", "/100/1/")).isTrue();
    }

    @Test
    void idsAreRootFirst() {
        assertThat(Paths.ids("/100/104/101/")).containsExactly(100L, 104L, 101L);
        assertThat(Paths.ids("/7/")).containsExactly(7L);
    }

    @Test
    void validation() {
        assertThat(Paths.isValid("/1/2/")).isTrue();
        assertThat(Paths.isValid("/1/2")).isFalse();
        assertThat(Paths.isValid("//")).isFalse();
        assertThat(Paths.isValid("/a/")).isFalse();
    }
}
