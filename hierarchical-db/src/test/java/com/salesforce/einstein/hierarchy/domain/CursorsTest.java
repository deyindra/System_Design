package com.salesforce.einstein.hierarchy.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CursorsTest {
    @Test
    void childCursorRoundTrips() {
        String c = Cursors.ofChild("i0z", 123);
        assertThat(c).doesNotContain("=", "+", "/");
        assertThat(Cursors.child(c)).isEqualTo(new Cursors.ChildKey("i0z", 123));
        assertThat(Cursors.child(null)).isNull();
    }

    @Test
    void pathCursorRoundTrips() {
        assertThat(Cursors.path(Cursors.ofPath("/1/2/3/"))).isEqualTo("/1/2/3/");
        assertThat(Cursors.path(" ")).isNull();
    }

    @Test
    void rejectsForeignOrTamperedCursors() {
        String path = Cursors.ofPath("/1/");
        assertThatThrownBy(() -> Cursors.child(path)).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> Cursors.path(Cursors.ofChild("i", 1))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> Cursors.child("!!!")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> Cursors.path(Cursors.ofPath("/1/x/"))).isInstanceOf(InvalidRequestException.class);
    }
}
