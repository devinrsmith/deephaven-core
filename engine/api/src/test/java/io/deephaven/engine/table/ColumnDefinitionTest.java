//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnDefinitionTest {
    private static final String COLUMN_NAME = "Foo";

    @Test
    public void ofBoolean() {
        final ColumnDefinition<Boolean> cd = ColumnDefinition.ofBoolean(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.booleanType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.booleanType())).isEqualTo(cd);
        // Note: ofBoolean produces Boolean.class dataType, which is different than the other primitive types
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, Boolean.class)).isEqualTo(cd);
    }

    @Test
    public void ofByte() {
        final ColumnDefinition<Byte> cd = ColumnDefinition.ofByte(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.byteType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.byteType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, byte.class)).isEqualTo(cd);
    }

    @Test
    public void ofChar() {
        final ColumnDefinition<Character> cd = ColumnDefinition.ofChar(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.charType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.charType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, char.class)).isEqualTo(cd);
    }

    @Test
    public void ofShort() {
        final ColumnDefinition<Short> cd = ColumnDefinition.ofShort(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.shortType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.shortType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, short.class)).isEqualTo(cd);
    }

    @Test
    public void ofInt() {
        final ColumnDefinition<Integer> cd = ColumnDefinition.ofInt(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.intType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.intType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, int.class)).isEqualTo(cd);
    }

    @Test
    public void ofLong() {
        final ColumnDefinition<Long> cd = ColumnDefinition.ofLong(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.longType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.longType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, long.class)).isEqualTo(cd);
    }

    @Test
    public void ofFloat() {
        final ColumnDefinition<Float> cd = ColumnDefinition.ofFloat(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.floatType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.floatType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, float.class)).isEqualTo(cd);
    }

    @Test
    public void ofDouble() {
        final ColumnDefinition<Double> cd = ColumnDefinition.ofDouble(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.doubleType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.doubleType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, double.class)).isEqualTo(cd);
    }

    @Test
    public void ofString() {
        final ColumnDefinition<String> cd = ColumnDefinition.ofString(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.stringType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.stringType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, String.class)).isEqualTo(cd);
    }

    @Test
    public void ofTime() {
        final ColumnDefinition<Instant> cd = ColumnDefinition.ofTime(COLUMN_NAME);
        assertThat(ColumnDefinition.of(COLUMN_NAME, Type.instantType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.instantType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, Instant.class)).isEqualTo(cd);
    }

    @Test
    public void ofBooleanArray() {
        // TODO
    }

    @Test
    public void ofByteArray() {
        final ColumnDefinition<byte[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.byteType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.byteType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.byteType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, byte[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, byte[].class, byte.class)).isEqualTo(cd);
    }

    @Test
    public void ofCharArray() {
        final ColumnDefinition<char[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.charType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.charType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.charType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, char[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, char[].class, char.class)).isEqualTo(cd);
    }

    @Test
    public void ofShortArray() {
        final ColumnDefinition<short[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.shortType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.shortType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.shortType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, short[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, short[].class, short.class)).isEqualTo(cd);
    }

    @Test
    public void ofIntArray() {
        final ColumnDefinition<int[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.intType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.intType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.intType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, int[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, int[].class, int.class)).isEqualTo(cd);
    }

    @Test
    public void ofLongArray() {
        final ColumnDefinition<long[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.longType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.longType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.longType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, long[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, long[].class, long.class)).isEqualTo(cd);
    }

    @Test
    public void ofFloatArray() {
        final ColumnDefinition<float[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.floatType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.floatType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.floatType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, float[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, float[].class, float.class)).isEqualTo(cd);
    }

    @Test
    public void ofDoubleArray() {
        final ColumnDefinition<double[]> cd = ColumnDefinition.of(COLUMN_NAME, Type.doubleType().arrayType());
        assertThat(ColumnDefinition.of(COLUMN_NAME, (GenericType<?>) Type.doubleType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.of(COLUMN_NAME, (Type<?>) Type.doubleType().arrayType())).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, double[].class)).isEqualTo(cd);
        assertThat(ColumnDefinition.fromGenericType(COLUMN_NAME, double[].class, double.class)).isEqualTo(cd);
    }
}
