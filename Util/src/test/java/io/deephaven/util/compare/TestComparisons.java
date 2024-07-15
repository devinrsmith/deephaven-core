//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;

public class TestComparisons {

    @Test
    public void testCharCharComparisons() {
        TestCase.assertTrue(CharComparisons.lt(QueryConstants.NULL_CHAR, 'A'));
        TestCase.assertTrue(CharComparisons.gt('A', QueryConstants.NULL_CHAR));
        TestCase.assertTrue(CharComparisons.eq(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertTrue(CharComparisons.lt('A', 'B'));
        TestCase.assertFalse(CharComparisons.gt('A', 'B'));
        TestCase.assertTrue(CharComparisons.eq(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertFalse(CharComparisons.eq('A', QueryConstants.NULL_CHAR));
    }

    @Test
    public void testCharArrayEquality() {
        TestCase.assertTrue(CharComparisons.eq(null, null));
        TestCase.assertTrue(CharComparisons.eq(new char[0], new char[0]));
        TestCase.assertTrue(CharComparisons.eq(
                new char[] {QueryConstants.NULL_CHAR, 'A'},
                new char[] {QueryConstants.NULL_CHAR, 'A'}));
    }

    @Test
    public void testCharArrayHashCode() {
        TestCase.assertEquals(
                Arrays.hashCode((char[]) null),
                CharComparisons.hashCode(null));
        TestCase.assertEquals(
                Arrays.hashCode(new char[0]),
                CharComparisons.hashCode(new char[0]));
        TestCase.assertEquals(
                Arrays.hashCode(new char[] {QueryConstants.NULL_CHAR, 'A'}),
                CharComparisons.hashCode(new char[] {QueryConstants.NULL_CHAR, 'A'}));
    }

    @Test
    public void testByteByteComparisons() {
        TestCase.assertTrue(ByteComparisons.lt(QueryConstants.NULL_BYTE, (byte) 2));
        TestCase.assertTrue(ByteComparisons.gt((byte) 2, QueryConstants.NULL_BYTE));
        TestCase.assertTrue(ByteComparisons.eq(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertTrue(ByteComparisons.lt((byte) 2, (byte) 3));
        TestCase.assertFalse(ByteComparisons.gt((byte) 2, (byte) 3));
        TestCase.assertTrue(ByteComparisons.eq(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertFalse(ByteComparisons.eq((byte) 2, QueryConstants.NULL_BYTE));
    }

    @Test
    public void testByteArrayEquality() {
        TestCase.assertTrue(ByteComparisons.eq(null, null));
        TestCase.assertTrue(ByteComparisons.eq(new byte[0], new byte[0]));
        TestCase.assertTrue(ByteComparisons.eq(
                new byte[] {QueryConstants.NULL_BYTE, (byte) 2},
                new byte[] {QueryConstants.NULL_BYTE, (byte) 2}));
    }

    @Test
    public void testByteArrayHashCode() {
        TestCase.assertEquals(
                Arrays.hashCode((byte[]) null),
                ByteComparisons.hashCode(null));
        TestCase.assertEquals(
                Arrays.hashCode(new byte[0]),
                ByteComparisons.hashCode(new byte[0]));
        TestCase.assertEquals(
                Arrays.hashCode(new byte[] {QueryConstants.NULL_BYTE, (byte) 2}),
                ByteComparisons.hashCode(new byte[] {QueryConstants.NULL_BYTE, (byte) 2}));
    }

    @Test
    public void testShortShortComparisons() {
        TestCase.assertTrue(ShortComparisons.lt(QueryConstants.NULL_SHORT, (short) 2));
        TestCase.assertTrue(ShortComparisons.gt((short) 2, QueryConstants.NULL_SHORT));
        TestCase.assertTrue(ShortComparisons.eq(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertTrue(ShortComparisons.lt((short) 2, (short) 3));
        TestCase.assertFalse(ShortComparisons.gt((short) 2, (short) 3));
        TestCase.assertTrue(ShortComparisons.eq(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertFalse(ShortComparisons.eq((short) 2, QueryConstants.NULL_SHORT));
    }

    @Test
    public void testShortArrayEquality() {
        TestCase.assertTrue(ShortComparisons.eq(null, null));
        TestCase.assertTrue(ShortComparisons.eq(new short[0], new short[0]));
        TestCase.assertTrue(ShortComparisons.eq(
                new short[] {QueryConstants.NULL_SHORT, (short) 2},
                new short[] {QueryConstants.NULL_SHORT, (short) 2}));
    }

    @Test
    public void testShortArrayHashCode() {
        TestCase.assertEquals(
                Arrays.hashCode((short[]) null),
                ShortComparisons.hashCode(null));
        TestCase.assertEquals(
                Arrays.hashCode(new short[0]),
                ShortComparisons.hashCode(new short[0]));
        TestCase.assertEquals(
                Arrays.hashCode(new short[] {QueryConstants.NULL_SHORT, (short) 2}),
                ShortComparisons.hashCode(new short[] {QueryConstants.NULL_SHORT, (short) 2}));
    }

    @Test
    public void testIntIntComparisons() {
        TestCase.assertTrue(IntComparisons.lt(QueryConstants.NULL_INT, (int) 2));
        TestCase.assertTrue(IntComparisons.gt(2, QueryConstants.NULL_INT));
        TestCase.assertTrue(IntComparisons.eq(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertTrue(IntComparisons.lt(2, 3));
        TestCase.assertFalse(IntComparisons.gt(2, 3));
        TestCase.assertTrue(IntComparisons.eq(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertFalse(IntComparisons.eq(2, QueryConstants.NULL_INT));
    }

    @Test
    public void testIntArrayEquality() {
        TestCase.assertTrue(IntComparisons.eq(null, null));
        TestCase.assertTrue(IntComparisons.eq(new int[0], new int[0]));
        TestCase.assertTrue(IntComparisons.eq(
                new int[] {QueryConstants.NULL_INT, 2},
                new int[] {QueryConstants.NULL_INT, 2}));
    }

    @Test
    public void testIntArrayHashCode() {
        TestCase.assertEquals(
                Arrays.hashCode((int[]) null),
                IntComparisons.hashCode(null));
        TestCase.assertEquals(
                Arrays.hashCode(new int[0]),
                IntComparisons.hashCode(new int[0]));
        TestCase.assertEquals(
                Arrays.hashCode(new int[] {QueryConstants.NULL_INT, 2}),
                IntComparisons.hashCode(new int[] {QueryConstants.NULL_INT, 2}));
    }

    @Test
    public void testLongLongComparisons() {
        TestCase.assertTrue(LongComparisons.lt(QueryConstants.NULL_LONG, 2));
        TestCase.assertTrue(LongComparisons.gt(2, QueryConstants.NULL_LONG));
        TestCase.assertTrue(LongComparisons.eq(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertTrue(LongComparisons.lt(2, 3));
        TestCase.assertFalse(LongComparisons.gt(2, 3));
        TestCase.assertTrue(LongComparisons.eq(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertFalse(LongComparisons.eq(2, QueryConstants.NULL_LONG));
    }

    @Test
    public void testLongArrayEquality() {
        TestCase.assertTrue(LongComparisons.eq(null, null));
        TestCase.assertTrue(LongComparisons.eq(new long[0], new long[0]));
        TestCase.assertTrue(LongComparisons.eq(
                new long[] {QueryConstants.NULL_LONG, 2L},
                new long[] {QueryConstants.NULL_LONG, 2L}));
    }

    @Test
    public void testLongArrayHashCode() {
        TestCase.assertEquals(
                Arrays.hashCode((long[]) null),
                LongComparisons.hashCode(null));
        TestCase.assertEquals(
                Arrays.hashCode(new long[0]),
                LongComparisons.hashCode(new long[0]));
        TestCase.assertEquals(
                Arrays.hashCode(new long[] {QueryConstants.NULL_LONG, 2L}),
                LongComparisons.hashCode(new long[] {QueryConstants.NULL_LONG, 2L}));
    }

    @Test
    public void testFloatFloatComparisons() {
        TestCase.assertTrue(FloatComparisons.lt(QueryConstants.NULL_FLOAT, 1.0f));
        TestCase.assertTrue(FloatComparisons.gt(2.0f, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatComparisons.eq(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatComparisons.lt(2.0f, 100f));
        TestCase.assertFalse(FloatComparisons.gt(2.0f, 100f));
        TestCase.assertTrue(FloatComparisons.eq(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertTrue(FloatComparisons.eq(Float.NaN, Float.NaN));
        TestCase.assertTrue(FloatComparisons.lt(QueryConstants.NULL_FLOAT, Float.NaN));
        TestCase.assertTrue(FloatComparisons.lt(1, Float.NaN));
        TestCase.assertTrue(FloatComparisons.eq(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.eq(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.POSITIVE_INFINITY, 7f));
        TestCase.assertTrue(FloatComparisons.lt(Float.NEGATIVE_INFINITY, 7f));
        TestCase.assertTrue(FloatComparisons.lt(7f, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(7f, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.eq(0.0f, -0.0f));
        TestCase.assertTrue(FloatComparisons.eq(-0.0f, 0.0f));
        TestCase.assertEquals(0, FloatComparisons.compare(0f, -0.0f));
        TestCase.assertEquals(0, FloatComparisons.compare(-0.0f, 0.0f));
        TestCase.assertTrue(FloatComparisons.lt(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertEquals(0, FloatComparisons.compare(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        TestCase.assertEquals(0, FloatComparisons.compare(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.NaN, Float.POSITIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.lt(Float.POSITIVE_INFINITY, Float.NaN));
        TestCase.assertTrue(FloatComparisons.lt(QueryConstants.NULL_FLOAT, Float.NEGATIVE_INFINITY));
        TestCase.assertTrue(FloatComparisons.gt(Float.NEGATIVE_INFINITY, QueryConstants.NULL_FLOAT));
    }

    @Test
    public void testFloatHashCode() {
        TestCase.assertEquals(FloatComparisons.hashCode(-0.0f), FloatComparisons.hashCode(0.0f));
        {
            final int nanHash = FloatComparisons.hashCode(Float.NaN);
            // These are the edges of float NaN representations, as documented by intBitsToFloat
            for (int nanEdge : new int[] {
                    0x7f800001,
                    0x7fffffff,
                    0xff800001,
                    0xffffffff
            }) {
                TestCase.assertEquals(nanHash, FloatComparisons.hashCode(Float.intBitsToFloat(nanEdge)));
            }
        }
    }

    @Test
    public void testFloatArrayEquality() {
        TestCase.assertTrue(FloatComparisons.eq(null, null));
        TestCase.assertTrue(FloatComparisons.eq(new float[0], new float[0]));
        TestCase.assertTrue(FloatComparisons.eq(
                new float[] {QueryConstants.NULL_FLOAT, Float.NaN, 0.0f, 0.0f, -0.0f, -0.0f},
                new float[] {QueryConstants.NULL_FLOAT, Float.NaN, 0.0f, -0.0f, 0.0f, -0.0f}));
    }

    @Test
    public void testFloatArrayHashCode() {
        // FloatComparisons has same structure as Arrays#hashCode(float[]), but canonicalizes -0.0f -> 0.0f
        final int expected = Arrays.hashCode(new float[] {QueryConstants.NULL_FLOAT, Float.NaN, 0.0f});
        TestCase.assertEquals(expected,
                FloatComparisons.hashCode(new float[] {QueryConstants.NULL_FLOAT, Float.NaN, 0.0f}));
        TestCase.assertEquals(expected,
                FloatComparisons.hashCode(new float[] {QueryConstants.NULL_FLOAT, Float.NaN, -0.0f}));
    }

    @Test
    public void testDoubleDoubleComparisons() {
        TestCase.assertTrue(DoubleComparisons.lt(QueryConstants.NULL_DOUBLE, 1.0));
        TestCase.assertTrue(DoubleComparisons.gt(2.0, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleComparisons.eq(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleComparisons.lt(2.0, 100));
        TestCase.assertFalse(DoubleComparisons.gt(2.0, 100));
        TestCase.assertTrue(DoubleComparisons.eq(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE));
        TestCase.assertTrue(DoubleComparisons.eq(Double.NaN, Double.NaN));
        TestCase.assertTrue(DoubleComparisons.lt(QueryConstants.NULL_DOUBLE, Double.NaN));
        TestCase.assertTrue(DoubleComparisons.lt(1, Double.NaN));
        TestCase.assertTrue(DoubleComparisons.eq(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.eq(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.gt(Double.POSITIVE_INFINITY, 7));
        TestCase.assertTrue(DoubleComparisons.lt(Double.NEGATIVE_INFINITY, 7));
        TestCase.assertTrue(DoubleComparisons.lt(7, Double.POSITIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.gt(7, Double.NEGATIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.eq(0.0, -0.0));
        TestCase.assertTrue(DoubleComparisons.eq(-0.0, 0.0));
        TestCase.assertEquals(0, DoubleComparisons.compare(0, -0.0));
        TestCase.assertEquals(0, DoubleComparisons.compare(-0.0, 0.0));
        TestCase.assertTrue(DoubleComparisons.lt(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.gt(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        TestCase.assertEquals(0, DoubleComparisons.compare(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY));
        TestCase.assertEquals(0, DoubleComparisons.compare(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.gt(Double.NaN, Double.POSITIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.lt(Double.POSITIVE_INFINITY, Double.NaN));
        TestCase.assertTrue(DoubleComparisons.lt(QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY));
        TestCase.assertTrue(DoubleComparisons.gt(Double.NEGATIVE_INFINITY, QueryConstants.NULL_DOUBLE));
    }

    @Test
    public void testDoubleHashCode() {
        TestCase.assertEquals(DoubleComparisons.hashCode(-0.0), DoubleComparisons.hashCode(0.0));
        {
            final int nanHash = DoubleComparisons.hashCode(Double.NaN);
            // These are the edges of double NaN representations, as documented by longBitsToDouble
            for (long nanEdge : new long[] {
                    0x7ff0000000000001L,
                    0x7fffffffffffffffL,
                    0xfff0000000000001L,
                    0xffffffffffffffffL
            }) {
                TestCase.assertEquals(nanHash, DoubleComparisons.hashCode(Double.longBitsToDouble(nanEdge)));
            }
        }
    }

    @Test
    public void testDoubleArrayEquality() {
        TestCase.assertTrue(DoubleComparisons.eq(null, null));
        TestCase.assertTrue(DoubleComparisons.eq(new double[0], new double[0]));
        TestCase.assertTrue(DoubleComparisons.eq(
                new double[] {QueryConstants.NULL_DOUBLE, Double.NaN, 0.0, 0.0, -0.0, -0.0},
                new double[] {QueryConstants.NULL_DOUBLE, Double.NaN, 0.0, -0.0, 0.0, -0.0}));
    }

    @Test
    public void testDoubleArrayHashCode() {
        // DoubleComparisons has same structure as Arrays#hashCode(double[]), but canonicalizes -0.0 -> 0.0
        final int expected = Arrays.hashCode(new double[] {QueryConstants.NULL_DOUBLE, Double.NaN, 0.0});
        TestCase.assertEquals(expected,
                DoubleComparisons.hashCode(new double[] {QueryConstants.NULL_DOUBLE, Double.NaN, 0.0}));
        TestCase.assertEquals(expected,
                DoubleComparisons.hashCode(new double[] {QueryConstants.NULL_DOUBLE, Double.NaN, -0.0}));
    }
}
