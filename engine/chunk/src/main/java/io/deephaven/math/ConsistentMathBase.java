//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Comparator;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

abstract class ConsistentMathBase implements ConsistentMath {

    @Override
    public final boolean equals(float[] x, float[] y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        return equals(x, 0, x.length, y, 0, y.length);
    }

    @Override
    public final boolean equals(double[] x, double[] y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        return equals(x, 0, x.length, y, 0, y.length);
    }

    @Override
    public final boolean equals(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        final int length = xTo - xFrom;
        if ((yTo - yFrom) != length) {
            return false;
        }
        return mismatch(x, xFrom, xTo, y, yFrom, yTo) == -1;
    }

    @Override
    public final boolean equals(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        final int length = xTo - xFrom;
        if ((yTo - yFrom) != length) {
            return false;
        }
        return mismatch(x, xFrom, xTo, y, yFrom, yTo) == -1;
    }

    @Override
    public final int hashCode(float[] x) {
        if (x == null) {
            return 0;
        }
        return hashCode(x, 0, x.length);
    }

    @Override
    public final int hashCode(double[] x) {
        if (x == null) {
            return 0;
        }
        return hashCode(x, 0, x.length);
    }

    @Override
    public final int hashCode(float[] x, int xFrom, int xTo) {
        int result = 1;
        for (int i = xFrom; i < xTo; ++i) {
            result = 31 * result + hashCode(x[i]);
        }
        return result;
    }

    @Override
    public final int hashCode(double[] x, int xFrom, int xTo) {
        int result = 1;
        for (int i = xFrom; i < xTo; ++i) {
            result = 31 * result + hashCode(x[i]);
        }
        return result;
    }

    @Override
    public final int compare(float[] x, float[] y) {
        if (x == y) {
            return 0;
        }
        if (x == null) {
            return -1;
        }
        if (y == null) {
            return 1;
        }
        return compare(x, 0, x.length, y, 0, y.length);
    }

    @Override
    public final int compare(double[] x, double[] y) {
        if (x == y) {
            return 0;
        }
        if (x == null) {
            return -1;
        }
        if (y == null) {
            return 1;
        }
        return compare(x, 0, x.length, y, 0, y.length);
    }

    @Override
    public final int compare(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int i = mismatch(x, xFrom, xTo, y, yFrom, yTo);
        return i >= 0 && xLength == yLength
                ? compare(x[xFrom + i], y[yFrom + i])
                : xLength - yLength;
    }

    @Override
    public final int compare(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int i = mismatch(x, xFrom, xTo, y, yFrom, yTo);
        return i >= 0 && xLength == yLength
                ? compare(x[xFrom + i], y[yFrom + i])
                : xLength - yLength;
    }

    @Override
    public final int mismatch(float[] x, float[] y) {
        final int length = Math.min(x.length, y.length); // Check null array refs
        if (x == y) {
            return -1;
        }
        return mismatch(x, 0, length, y, 0, length);
    }

    @Override
    public final int mismatch(double[] x, double[] y) {
        final int length = Math.min(x.length, y.length); // Check null array refs
        if (x == y) {
            return -1;
        }
        return mismatch(x, 0, length, y, 0, length);
    }

    @Override
    public final int mismatch(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int length = Math.min(xLength, yLength);
        for (int i = 0; i < length; ++i) {
            if (!equals(x[xFrom + i], y[yFrom + i])) {
                return i;
            }
        }
        return xLength == yLength ? -1 : length;
    }

    @Override
    public final int mismatch(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        final int xLength = xTo - xFrom;
        final int yLength = yTo - yFrom;
        final int length = Math.min(xLength, yLength);
        for (int i = 0; i < length; ++i) {
            if (!equals(x[xFrom + i], y[yFrom + i])) {
                return i;
            }
        }
        return xLength == yLength ? -1 : length;
    }

    @Override
    public final <T> BiPredicate<T, T> equals(Class<T> clazz) {
        return ConsistentMathImpl.predicate(this, clazz, false);
    }

    @Override
    public final <T> BiPredicate<T, T> deepEquals(Class<T> clazz) {
        return ConsistentMathImpl.predicate(this, clazz, true);
    }

    @Override
    public final <T> ToIntFunction<T> hashCode(Class<T> clazz) {
        return ConsistentMathImpl.hasher(this, clazz, false);
    }

    @Override
    public final <T> ToIntFunction<T> deepHashCode(Class<T> clazz) {
        return ConsistentMathImpl.hasher(this, clazz, true);
    }

    @Override
    public final <T> Comparator<T> comparator(Class<T> clazz) {
        return ConsistentMathImpl.comparator(this, clazz);
    }
}
