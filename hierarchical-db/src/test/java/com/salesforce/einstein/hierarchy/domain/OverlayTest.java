package com.salesforce.einstein.hierarchy.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class OverlayTest {
    // DESIGN.md §4.3: move 101 (Architecture) under 104 (Runbooks).
    private final Overlay o = new Overlay(9, "/100/101/", "/100/104/101/", 1);

    @Test
    void translatesUnmigratedRows() {
        assertThat(o.applies("/100/101/102/")).isTrue();
        assertThat(o.path("/100/101/102/")).isEqualTo("/100/104/101/102/");
        assertThat(o.depth("/100/101/102/", 2)).isEqualTo(3);
    }

    @Test
    void leavesOtherRowsAlone() {
        assertThat(o.applies("/100/104/101/102/")).isFalse();   // already rewritten
        assertThat(o.applies("/100/1010/")).isFalse();          // a sibling whose id merely starts with 101
        assertThat(o.path("/100/104/105/")).isEqualTo("/100/104/105/");
        assertThat(Overlay.NONE.isActive()).isFalse();
        assertThat(Overlay.NONE.applies("/1/")).isFalse();
    }

    @Test
    void appliesDepthDelta() {
        Overlay deeper = new Overlay(1, "/1/2/", "/1/3/4/2/", 1);
        Node n = new Node(5, 1, 2L, "/1/2/5/", 2, "i", "page", "t", NodeStatus.ACTIVE, 1, "u", Instant.EPOCH,
                Instant.EPOCH);
        Node e = deeper.apply(n);
        assertThat(e.path()).isEqualTo("/1/3/4/2/5/");
        assertThat(e.depth()).isEqualTo(3);
        assertThat(Overlay.NONE.apply(n)).isSameAs(n);
    }
}
