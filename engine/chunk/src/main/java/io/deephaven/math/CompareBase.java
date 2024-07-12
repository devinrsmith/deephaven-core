//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

abstract class CompareBase implements Compare {
    private static final Comparator<boolean[]> BOOLEAN_ARRAY_COMPARE = Arrays::compare;
    private static final Comparator<byte[]> BYTE_ARRAY_COMPARE = Arrays::compare;
    private static final Comparator<short[]> SHORT_ARRAY_COMPARE = Arrays::compare;
    private static final Comparator<int[]> INT_ARRAY_COMPARE = Arrays::compare;
    private static final Comparator<long[]> LONG_ARRAY_COMPARE = Arrays::compare;

    private final Equals math;
    private final Comparator<char[]> compareCharArray;
    private final Comparator<float[]> compareFloatArray;
    private final Comparator<double[]> compareDoubleArray;

    CompareBase(Equals math) {
        this.math = Objects.requireNonNull(math);
        this.compareCharArray = this::compare;
        this.compareFloatArray = this::compare;
        this.compareDoubleArray = this::compare;
    }

    @Override
    public final Equals math() {
        return math;
    }

    @Override
    public final <T> Comparator<T> comparator(Class<T> clazz) {
        if (!clazz.isArray()) {
            if (clazz.isPrimitive()) {
                throw new IllegalArgumentException("Primitive types not supported");
            }
            if (!Comparable.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Leaf type must be Comparable");
            }
            // noinspection unchecked
            return (Comparator<T>) Comparator.naturalOrder();
        }
        if (boolean[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) BOOLEAN_ARRAY_COMPARE;
        }
        if (byte[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) BYTE_ARRAY_COMPARE;
        }
        if (char[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) compareCharArray;
        }
        if (short[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) SHORT_ARRAY_COMPARE;
        }
        if (int[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) INT_ARRAY_COMPARE;
        }
        if (long[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) LONG_ARRAY_COMPARE;
        }
        if (float[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) compareFloatArray;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) compareDoubleArray;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            // noinspection unchecked
            return (Comparator<T>) new ArrayComparator<>(comparator(clazz.getComponentType()));
        }
        throw new IllegalStateException();
    }

    static int compare(Compare compare, char[] x, char[] y) {
        if (x == y) {
            return 0;
        }
        if (x == null) {
            return -1;
        }
        if (y == null) {
            return 1;
        }
        return compare(compare, x, 0, x.length, y, 0, y.length);
    }

    static int compare(Compare compare, float[] x, float[] y) {
        if (x == y) {
            return 0;
        }
        if (x == null) {
            return -1;
        }
        if (y == null) {
            return 1;
        }
        return compare(compare, x, 0, x.length, y, 0, y.length);
    }

    static int compare(Compare compare, double[] x, double[] y) {
        if (x == y) {
            return 0;
        }
        if (x == null) {
            return -1;
        }
        if (y == null) {
            return 1;
        }
        return compare(compare, x, 0, x.length, y, 0, y.length);
    }

    static int compare(Compare compare, char[] x, int xFrom, int xTo, char[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int i = Arrays.mismatch(x, xFrom, xTo, y, yFrom, yTo);
        return i >= 0 && xLength == yLength
                ? compare.compare(x[xFrom + i], y[yFrom + i])
                : xLength - yLength;
    }

    static int compare(Compare compare, float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int i = compare.math().mismatch(x, xFrom, xTo, y, yFrom, yTo);
        return i >= 0 && xLength == yLength
                ? compare.compare(x[xFrom + i], y[yFrom + i])
                : xLength - yLength;
    }

    static int compare(Compare compare, double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int i = compare.math().mismatch(x, xFrom, xTo, y, yFrom, yTo);
        return i >= 0 && xLength == yLength
                ? compare.compare(x[xFrom + i], y[yFrom + i])
                : xLength - yLength;
    }
}
