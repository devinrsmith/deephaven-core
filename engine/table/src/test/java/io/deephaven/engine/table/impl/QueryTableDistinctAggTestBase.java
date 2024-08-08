//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSet.Iterator;
import io.deephaven.engine.table.ColumnSource;
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
public abstract class QueryTableDistinctAggTestBase {

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

    // We are combining the test cases for distinct and countDistinct because the test vectors can be re-used (by simply
    // counting the length of the expected distincts)

    public abstract Table distinct(boolean includeNullAndNans, char[] source);

    public abstract Table distinct(boolean includeNullAndNans, float[] source);

    public abstract Table distinct(boolean includeNullAndNans, double[] source);

    public abstract Table countDistinct(boolean includeNullAndNans, char[] source);

    public abstract Table countDistinct(boolean includeNullAndNans, float[] source);

    public abstract Table countDistinct(boolean includeNullAndNans, double[] source);

    @Test
    public void testEmptyChar() {
        assertThat(distinct(true, new char[0]).isEmpty()).isTrue();
        assertThat(distinct(false, new char[0]).isEmpty()).isTrue();

        assertThat(countDistinct(true, new char[0]).isEmpty()).isTrue();
        assertThat(countDistinct(false, new char[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyFloat() {
        assertThat(distinct(true, new float[0]).isEmpty()).isTrue();
        assertThat(distinct(false, new float[0]).isEmpty()).isTrue();

        assertThat(countDistinct(true, new float[0]).isEmpty()).isTrue();
        assertThat(countDistinct(false, new float[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyDouble() {
        assertThat(distinct(true, new double[0]).isEmpty()).isTrue();
        assertThat(distinct(false, new double[0]).isEmpty()).isTrue();

        assertThat(countDistinct(true, new double[0]).isEmpty()).isTrue();
        assertThat(countDistinct(false, new double[0]).isEmpty()).isTrue();
    }

    @Test
    public void testAllNullChar() {
        check(new char[] {NULL_CHAR}, true, new char[] {NULL_CHAR, NULL_CHAR});
        check(new char[] {}, false, new char[] {NULL_CHAR, NULL_CHAR});
    }

    @Test
    public void testAllNullNanFloat() {
        check(new float[] {NULL_FLOAT}, true, new float[] {NULL_FLOAT, NULL_FLOAT});
        check(new float[] {}, false, new float[] {NULL_FLOAT, NULL_FLOAT});

        check(new float[] {Float.NaN}, true, new float[] {Float.NaN, Float.NaN});
        check(new float[] {}, false, new float[] {Float.NaN, Float.NaN});

        check(new float[] {NULL_FLOAT, Float.NaN}, true, new float[] {Float.NaN, NULL_FLOAT, Float.NaN, NULL_FLOAT});
        check(new float[] {}, false, new float[] {Float.NaN, NULL_FLOAT, Float.NaN, NULL_FLOAT});
    }

    @Test
    public void testAllNullNanDouble() {
        check(new double[] {NULL_DOUBLE}, true, new double[] {NULL_DOUBLE, NULL_DOUBLE});
        check(new double[] {}, false, new double[] {NULL_DOUBLE, NULL_DOUBLE});

        check(new double[] {Double.NaN}, true, new double[] {Double.NaN, Double.NaN});
        check(new double[] {}, false, new double[] {Double.NaN, Double.NaN});

        check(new double[] {NULL_DOUBLE, Double.NaN}, true,
                new double[] {Double.NaN, NULL_DOUBLE, Double.NaN, NULL_DOUBLE});
        check(new double[] {}, false, new double[] {Double.NaN, NULL_DOUBLE, Double.NaN, NULL_DOUBLE});
    }

    @Test
    public void testZerosFloat() {
        check(new float[] {-0.0f}, true, new float[] {0.0f, -0.0f});
        check(new float[] {-0.0f}, false, new float[] {0.0f, -0.0f});
    }

    @Test
    public void testZerosDouble() {
        check(new double[] {-0.0}, true, new double[] {0.0, -0.0});
        check(new double[] {-0.0}, false, new double[] {0.0, -0.0});
    }

    @Test
    public void testChar() {
        check(new char[] {NULL_CHAR, Character.MIN_VALUE, 'a', Character.MAX_VALUE - 1}, true,
                new char[] {NULL_CHAR, 'a', NULL_CHAR, 'a', Character.MIN_VALUE, Character.MAX_VALUE - 1});
        check(new char[] {Character.MIN_VALUE, 'a', Character.MAX_VALUE - 1}, false,
                new char[] {NULL_CHAR, 'a', NULL_CHAR, 'a', Character.MIN_VALUE, Character.MAX_VALUE - 1});
    }

    @Test
    public void testFloat() {
        check(new float[] {NULL_FLOAT, Float.NEGATIVE_INFINITY, 42.0f, Float.POSITIVE_INFINITY, Float.NaN}, true,
                new float[] {Float.NaN, NULL_FLOAT, 42.0f, NULL_FLOAT, 42.0f, Float.NEGATIVE_INFINITY,
                        Float.POSITIVE_INFINITY});
        check(new float[] {Float.NEGATIVE_INFINITY, 42.0f, Float.POSITIVE_INFINITY}, false, new float[] {Float.NaN,
                NULL_FLOAT, 42.0f, NULL_FLOAT, 42.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY});
    }

    @Test
    public void testDouble() {
        check(new double[] {NULL_DOUBLE, Double.NEGATIVE_INFINITY, 42.0, Double.POSITIVE_INFINITY, Double.NaN}, true,
                new double[] {Double.NaN, NULL_DOUBLE, 42.0, NULL_DOUBLE, 42.0, Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY});
        check(new double[] {Double.NEGATIVE_INFINITY, 42.0, Double.POSITIVE_INFINITY}, false, new double[] {Double.NaN,
                NULL_DOUBLE, 42.0, NULL_DOUBLE, 42.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY});
    }

    public void check(char[] expected, boolean includeNullAndNans, char[] data) {
        checkImpl(expected, includeNullAndNans, data);
        for (int times = 16; times <= 1024; times *= 2) {
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times - 1, data));
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times, data));
        }
    }

    public void check(float[] expected, boolean includeNullAndNans, float[] data) {
        checkImpl(expected, includeNullAndNans, data);
        for (int times = 16; times <= 1024; times *= 2) {
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times - 1, data));
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times, data));
        }
    }

    public void check(double[] expected, boolean includeNullAndNans, double[] data) {
        checkImpl(expected, includeNullAndNans, data);
        for (int times = 16; times <= 1024; times *= 2) {
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times - 1, data));
            checkImpl(expected, includeNullAndNans, ArrayUtilities.duplicate(times, data));
        }
    }

    private void checkImpl(char[] expected, boolean includeNullAndNans, char[] data) {
        final Table actual = distinct(includeNullAndNans, data);
        final ColumnSource<Character> source = actual.getColumnSource(S1, char.class);
        assertThat(actual.size()).isEqualTo(expected.length);
        try (final Iterator key = actual.getRowSet().iterator()) {
            for (int i = 0; i < expected.length; ++i) {
                assertThat(key.hasNext()).isTrue();
                assertThat(source.getChar(key.nextLong())).isEqualTo(expected[i]);
            }
            assertThat(key.hasNext()).isFalse();
        }
        checkCountDistinctImpl(expected.length == 0 ? NULL_LONG : expected.length, includeNullAndNans, data);
    }

    private void checkImpl(float[] expected, boolean includeNullAndNans, float[] data) {
        final Table actual = distinct(includeNullAndNans, data);
        final ColumnSource<Float> source = actual.getColumnSource(S1, float.class);
        assertThat(actual.size()).isEqualTo(expected.length);
        try (final Iterator key = actual.getRowSet().iterator()) {
            for (int i = 0; i < expected.length; ++i) {
                assertThat(key.hasNext()).isTrue();
                assertThat(source.getFloat(key.nextLong())).usingComparator(Float::compare).isEqualTo(expected[i]);
            }
            assertThat(key.hasNext()).isFalse();
        }
        checkCountDistinctImpl(expected.length == 0 ? NULL_LONG : expected.length, includeNullAndNans, data);
    }

    private void checkImpl(double[] expected, boolean includeNullAndNans, double[] data) {
        final Table actual = distinct(includeNullAndNans, data);
        final ColumnSource<Double> source = actual.getColumnSource(S1, double.class);
        assertThat(actual.size()).isEqualTo(expected.length);
        try (final Iterator key = actual.getRowSet().iterator()) {
            for (int i = 0; i < expected.length; ++i) {
                assertThat(key.hasNext()).isTrue();
                assertThat(source.getDouble(key.nextLong())).usingComparator(Double::compare).isEqualTo(expected[i]);
            }
            assertThat(key.hasNext()).isFalse();
        }
        checkCountDistinctImpl(expected.length == 0 ? NULL_LONG : expected.length, includeNullAndNans, data);
    }

    private void checkCountDistinctImpl(long expected, boolean includeNullAndNans, char[] data) {
        assertThat(longValue(countDistinct(includeNullAndNans, data))).isEqualTo(expected);
    }

    private void checkCountDistinctImpl(long expected, boolean includeNullAndNans, float[] data) {
        assertThat(longValue(countDistinct(includeNullAndNans, data))).isEqualTo(expected);
    }

    private void checkCountDistinctImpl(long expected, boolean includeNullAndNans, double[] data) {
        assertThat(longValue(countDistinct(includeNullAndNans, data))).isEqualTo(expected);
    }

    private static long longValue(Table x) {
        assertThat(x.size()).isEqualTo(1);
        return x.getColumnSource(S1, long.class).getLong(x.getRowSet().firstRowKey());
    }
}
