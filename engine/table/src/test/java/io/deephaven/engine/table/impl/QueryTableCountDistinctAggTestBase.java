//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.assertj.core.api.Assertions.assertThat;

@Category(OutOfBandTest.class)
public abstract class QueryTableCountDistinctAggTestBase {

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

    public abstract Table countDistinct(boolean includeNullAndNans, char[] source);

    public abstract Table countDistinct(boolean includeNullAndNans, float[] source);

    public abstract Table countDistinct(boolean includeNullAndNans, double[] source);

    @Test
    public void testEmptyChar() {
        assertThat(countDistinct(true, new char[0]).isEmpty()).isTrue();
        assertThat(countDistinct(false, new char[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyFloat() {
        assertThat(countDistinct(true, new float[0]).isEmpty()).isTrue();
        assertThat(countDistinct(false, new float[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyDouble() {
        assertThat(countDistinct(true, new double[0]).isEmpty()).isTrue();
        assertThat(countDistinct(false, new double[0]).isEmpty()).isTrue();
    }

    @Test
    public void testAllNullChar() {
        check(1, true, new char[] {NULL_CHAR, NULL_CHAR});
        check(NULL_LONG, false, new char[] {NULL_CHAR, NULL_CHAR});
    }

    @Test
    public void testAllNullNanFloat() {
        check(1, true, new float[] {NULL_FLOAT, NULL_FLOAT});
        check(NULL_LONG, false, new float[] {NULL_FLOAT, NULL_FLOAT});

        check(1, true, new float[] {Float.NaN, Float.NaN});
        check(NULL_LONG, false, new float[] {Float.NaN, Float.NaN});

        check(2, true, new float[] {Float.NaN, NULL_FLOAT, Float.NaN, NULL_FLOAT});
        check(NULL_LONG, false, new float[] {Float.NaN, NULL_FLOAT, Float.NaN, NULL_FLOAT});
    }

    @Test
    public void testAllNullNanDouble() {
        check(1, true, new double[] {NULL_DOUBLE, NULL_DOUBLE});
        check(NULL_LONG, false, new double[] {NULL_DOUBLE, NULL_DOUBLE});

        check(1, true, new double[] {Double.NaN, Double.NaN});
        check(NULL_LONG, false, new double[] {Double.NaN, Double.NaN});

        check(2, true, new double[] {Double.NaN, NULL_DOUBLE, Double.NaN, NULL_DOUBLE});
        check(NULL_LONG, false, new double[] {Double.NaN, NULL_DOUBLE, Double.NaN, NULL_DOUBLE});
    }

    @Test
    public void testZerosFloat() {
        check(1, true, new float[] {0.0f, -0.0f});
        check(1, false, new float[] {0.0f, -0.0f});
    }

    @Test
    public void testZerosDouble() {
        check(1, true, new double[] {0.0, -0.0});
        check(1, false, new double[] {0.0, -0.0});
    }

    @Test
    public void testChar() {
        check(4, true, new char[] {NULL_CHAR, 'a', NULL_CHAR, 'a', Character.MIN_VALUE, Character.MAX_VALUE - 1});
        check(3, false, new char[] {NULL_CHAR, 'a', NULL_CHAR, 'a', Character.MIN_VALUE, Character.MAX_VALUE - 1});
    }

    @Test
    public void testFloat() {
        check(5, true, new float[] {Float.NaN, NULL_FLOAT, 42.0f, NULL_FLOAT, 42.0f, Float.NEGATIVE_INFINITY,
                Float.POSITIVE_INFINITY});
        check(3, false, new float[] {Float.NaN, NULL_FLOAT, 42.0f, NULL_FLOAT, 42.0f, Float.NEGATIVE_INFINITY,
                Float.POSITIVE_INFINITY});
    }

    @Test
    public void testDouble() {
        check(5, true, new double[] {Double.NaN, NULL_DOUBLE, 42.0, NULL_DOUBLE, 42.0, Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY});
        check(3, false, new double[] {Double.NaN, NULL_DOUBLE, 42.0, NULL_DOUBLE, 42.0, Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY});
    }

    public void check(long expected, boolean includeNullAndNans, char[] data) {
        checkImpl(expected, includeNullAndNans, data);
        for (int times = 16; times <= 1024; times *= 2) {
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times - 1, data));
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times, data));
        }
    }

    public void check(long expected, boolean includeNullAndNans, float[] data) {
        checkImpl(expected, includeNullAndNans, data);
        for (int times = 16; times <= 1024; times *= 2) {
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times - 1, data));
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times, data));
        }
    }

    public void check(long expected, boolean includeNullAndNans, double[] data) {
        checkImpl(expected, includeNullAndNans, data);
        for (int times = 16; times <= 1024; times *= 2) {
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times - 1, data));
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times, data));
        }
    }

    private void checkImpl(long expected, boolean includeNullAndNans, char[] data) {
        assertThat(longValue(countDistinct(includeNullAndNans, data))).isEqualTo(expected);
    }

    private void checkImpl(long expected, boolean includeNullAndNans, float[] data) {
        assertThat(longValue(countDistinct(includeNullAndNans, data))).isEqualTo(expected);
    }

    private void checkImpl(long expected, boolean includeNullAndNans, double[] data) {
        assertThat(longValue(countDistinct(includeNullAndNans, data))).isEqualTo(expected);
    }

    private static long longValue(Table x) {
        assertThat(x.size()).isEqualTo(1);
        return x.getColumnSource(S1, long.class).getLong(x.getRowSet().firstRowKey());
    }

}
