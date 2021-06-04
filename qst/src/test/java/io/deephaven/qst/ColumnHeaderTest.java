package io.deephaven.qst;

import static io.deephaven.qst.ColumnHeader.ofBoolean;
import static io.deephaven.qst.ColumnHeader.ofByte;
import static io.deephaven.qst.ColumnHeader.ofChar;
import static io.deephaven.qst.ColumnHeader.ofDouble;
import static io.deephaven.qst.ColumnHeader.ofFloat;
import static io.deephaven.qst.ColumnHeader.ofInt;
import static io.deephaven.qst.ColumnHeader.ofLong;
import static io.deephaven.qst.ColumnHeader.ofShort;
import static io.deephaven.qst.ColumnType.booleanType;
import static io.deephaven.qst.ColumnType.byteType;
import static io.deephaven.qst.ColumnType.charType;
import static io.deephaven.qst.ColumnType.doubleType;
import static io.deephaven.qst.ColumnType.floatType;
import static io.deephaven.qst.ColumnType.intType;
import static io.deephaven.qst.ColumnType.longType;
import static io.deephaven.qst.ColumnType.shortType;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ColumnHeaderTest {

    @Test
    void name() {
        assertThat(ColumnHeader.of("TheName", intType()).name()).isEqualTo("TheName");
    }

    @Test
    void ofBooleanType() {
        assertThat(ofBoolean("X").type()).isEqualTo(booleanType());
    }

    @Test
    void ofByteType() {
        assertThat(ofByte("X").type()).isEqualTo(byteType());
    }

    @Test
    void ofCharType() {
        assertThat(ofChar("X").type()).isEqualTo(charType());
    }

    @Test
    void ofShortType() {
        assertThat(ofShort("X").type()).isEqualTo(shortType());
    }

    @Test
    void ofIntType() {
        assertThat(ofInt("X").type()).isEqualTo(intType());
    }

    @Test
    void ofLongType() {
        assertThat(ofLong("X").type()).isEqualTo(longType());
    }

    @Test
    void ofFloatType() {
        assertThat(ofFloat("X").type()).isEqualTo(floatType());
    }

    @Test
    void ofDoubleType() {
        assertThat(ofDouble("X").type()).isEqualTo(doubleType());
    }
}
