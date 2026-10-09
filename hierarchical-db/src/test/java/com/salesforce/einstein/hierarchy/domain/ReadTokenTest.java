package com.salesforce.einstein.hierarchy.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReadTokenTest {
    @Test
    void roundTripsPostgresLsnText() {
        long lsn = ReadToken.parseLsn("0/16B3748");
        assertThat(lsn).isEqualTo(0x16B3748L);
        assertThat(ReadToken.formatLsn(lsn)).isEqualTo("0/16B3748");
        assertThat(ReadToken.parseLsn("1/0")).isEqualTo(1L << 32);
        ReadToken t = new ReadToken("s0", ReadToken.parseLsn("A/FF"));
        assertThat(t.format()).isEqualTo("s0:A/FF");
        assertThat(ReadToken.parse(t.format())).isEqualTo(t);
    }

    @Test
    void malformedTokensMeanNoToken() {
        assertThat(ReadToken.parse(null)).isEqualTo(ReadToken.NONE);
        assertThat(ReadToken.parse("")).isEqualTo(ReadToken.NONE);
        assertThat(ReadToken.parse("garbage")).isEqualTo(ReadToken.NONE);
        assertThat(ReadToken.parse("s0:xyz")).isEqualTo(ReadToken.NONE);
        assertThat(ReadToken.parse("s0:0/0")).isEqualTo(ReadToken.NONE);
        assertThat(ReadToken.parse(":0/1")).isEqualTo(ReadToken.NONE);
        assertThatThrownBy(() -> ReadToken.parseLsn("1FFFFFFFF/0")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maxPicksTheLater() {
        ReadToken a = new ReadToken("s0", 5);
        ReadToken b = new ReadToken("s0", 9);
        assertThat(a.max(b)).isEqualTo(b);
        assertThat(b.max(a)).isEqualTo(b);
        assertThat(a.max(ReadToken.NONE)).isEqualTo(a);
        assertThat(ReadToken.NONE.max(a)).isEqualTo(a);
    }
}
