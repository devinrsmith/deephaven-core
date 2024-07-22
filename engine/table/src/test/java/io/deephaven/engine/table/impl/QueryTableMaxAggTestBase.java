//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.assertj.core.api.AbstractDoubleAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

@Category(OutOfBandTest.class)
public abstract class QueryTableMaxAggTestBase {

    static final String S1 = "S1";

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    public abstract Table max(char[] source);

    public abstract Table max(float[] source);

    public abstract Table max(double[] source);

    @Test
    public void testEmptyChar() {
        assertThat(max(new char[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyFloat() {
        assertThat(max(new float[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyDouble() {
        assertThat(max(new double[0]).isEmpty()).isTrue();
    }

    @Test
    public void testAllNullChar() {
        check(QueryConstants.NULL_CHAR, new char[] {QueryConstants.NULL_CHAR});
    }

    @Test
    public void testAllNullFloat() {
        check(QueryConstants.NULL_FLOAT, new float[] {QueryConstants.NULL_FLOAT});
    }

    @Test
    public void testAllNullDouble() {
        check(QueryConstants.NULL_DOUBLE, new double[] {QueryConstants.NULL_DOUBLE});
    }

    @Test
    public void testSkipsNullChar() {
        check(Character.MIN_VALUE, new char[] {QueryConstants.NULL_CHAR, Character.MIN_VALUE});
    }

    @Test
    public void testSkipsNullFloat() {
        check(Float.NEGATIVE_INFINITY, new float[] {QueryConstants.NULL_FLOAT, Float.NEGATIVE_INFINITY});
    }

    @Test
    public void testSkipsNullDouble() {
        check(Double.NEGATIVE_INFINITY, new double[] {QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY});
    }

    @Test
    public void testSkipsNanFloat() {
        check(Float.NEGATIVE_INFINITY, new float[] {Float.NEGATIVE_INFINITY, Float.NaN});
    }

    @Test
    public void testSkipsNanDouble() {
        check(Double.NEGATIVE_INFINITY, new double[] {Double.NEGATIVE_INFINITY, Double.NaN});
    }

    @Test
    public void testNegZeroFirstFloat() {
        check(-0.0f, new float[] {-0.0f, 0.0f});
    }

    @Test
    public void testNegZeroFirstDouble() {
        check(-0.0, new double[] {-0.0, 0.0});
    }

    @Test
    public void testZeroFirstFloat() {
        check(0.0f, new float[] {0.0f, -0.0f});
    }

    @Test
    public void testZeroFirstDouble() {
        check(0.0, new double[] {0.0, -0.0});
    }

    @Test
    public void testAllNaNFloat() {
        check(QueryConstants.NULL_FLOAT, new float[] {Float.NaN});
    }

    @Test
    public void testAllNaNDouble() {
        check(QueryConstants.NULL_DOUBLE, new double[] {Double.NaN});
    }

    public void check(char expected, char[] data) {
        assertThat(charValue(max(data))).isEqualTo(expected);
    }

    public void check(float expected, float[] data) {
        at(floatValue(max(data))).isEqualTo(expected);
    }

    public void check(double expected, double[] data) {
        at(doubleValue(max(data))).isEqualTo(expected);
    }

    private static char charValue(Table x) {
        assertThat(x.size()).isEqualTo(1);
        return x.getColumnSource(S1, char.class).getChar(x.getRowSet().firstRowKey());
    }

    private static float floatValue(Table x) {
        assertThat(x.size()).isEqualTo(1);
        return x.getColumnSource(S1, float.class).getFloat(x.getRowSet().firstRowKey());
    }

    private static double doubleValue(Table x) {
        assertThat(x.size()).isEqualTo(1);
        return x.getColumnSource(S1, double.class).getDouble(x.getRowSet().firstRowKey());
    }

    private static AbstractDoubleAssert<?> at(double x) {
        return assertThat(x).usingComparator(Double::compareTo);
    }
}
