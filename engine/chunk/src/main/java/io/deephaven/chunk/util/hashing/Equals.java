//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;

import java.util.Arrays;
import java.util.function.BiPredicate;

final class Equals {

    private static final BiPredicate<boolean[], boolean[]> BOOLEAN_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<char[], char[]> CHAR_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<byte[], byte[]> BYTE_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<short[], short[]> SHORT_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<int[], int[]> INT_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<long[], long[]> LONG_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<float[], float[]> FLOAT_ARRAY_EQUALS = Equals::equals;
    private static final BiPredicate<double[], double[]> DOUBLE_ARRAY_EQUALS = Equals::equals;

    public static boolean equals(boolean[] x, boolean[] y) {
        return Arrays.equals(x, y);
    }

    public static boolean equals(byte[] x, byte[] y) {
        return Arrays.equals(x, y);
    }

    public static boolean equals(char[] x, char[] y) {
        return Arrays.equals(x, y);
    }

    public static boolean equals(short[] x, short[] y) {
        return Arrays.equals(x, y);
    }

    public static boolean equals(int[] x, int[] y) {
        return Arrays.equals(x, y);
    }

    public static boolean equals(long[] x, long[] y) {
        return Arrays.equals(x, y);
    }

    public static boolean equals(float[] x, float[] y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        final int length = x.length;
        if (y.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!FloatComparisons.eq(x[i], y[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(double[] x, double[] y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        final int length = x.length;
        if (y.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!DoubleComparisons.eq(x[i], y[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(Object[] x, Object[] y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        final int length = x.length;
        if (y.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!deepEquals0(x, y)) {
                return false;
            }
        }
        return true;
    }

    private static boolean deepEquals0(Object x, Object y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        if (x instanceof Object[] && y instanceof Object[]) {
            return equals((Object[]) x, (Object[]) y);
        } else if (x instanceof byte[] && y instanceof byte[]) {
            return equals((byte[]) x, (byte[]) y);
        } else if (x instanceof short[] && y instanceof short[]) {
            return equals((short[]) x, (short[]) y);
        } else if (x instanceof int[] && y instanceof int[]) {
            return equals((int[]) x, (int[]) y);
        } else if (x instanceof long[] && y instanceof long[]) {
            return equals((long[]) x, (long[]) y);
        } else if (x instanceof char[] && y instanceof char[]) {
            return equals((char[]) x, (char[]) y);
        } else if (x instanceof float[] && y instanceof float[]) {
            return equals((float[]) x, (float[]) y);
        } else if (x instanceof double[] && y instanceof double[]) {
            return equals((double[]) x, (double[]) y);
        } else if (x instanceof boolean[] && y instanceof boolean[]) {
            return equals((boolean[]) x, (boolean[]) y);
        } else {
            return x.equals(y);
        }
    }
}
