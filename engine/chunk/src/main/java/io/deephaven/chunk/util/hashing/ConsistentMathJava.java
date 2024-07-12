//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import java.util.Arrays;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

// NaNs are all equal
// -0.0 / 0.0 are not equal
enum ConsistentMathJava implements ConsistentMath {
    JAVA;

    @Override
    public boolean equals(float x, float y) {
        return Float.floatToIntBits(x) == Float.floatToIntBits(y);
    }

    @Override
    public boolean equals(double x, double y) {
        return Double.doubleToLongBits(x) == Double.doubleToLongBits(y);
    }

    @Override
    public boolean equals(float[] x, float[] y) {
        return Arrays.equals(x, y);
    }

    @Override
    public boolean equals(double[] x, double[] y) {
        return Arrays.equals(x, y);
    }

    @Override
    public boolean equals(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        return Arrays.equals(x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public boolean equals(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        return Arrays.equals(x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int hashCode(float x) {
        return Float.hashCode(x);
    }

    @Override
    public int hashCode(double x) {
        return Double.hashCode(x);
    }

    @Override
    public int hashCode(float[] x) {
        return Arrays.hashCode(x);
    }

    @Override
    public int hashCode(double[] x) {
        return Arrays.hashCode(x);
    }

    @Override
    public int hashCode(float[] x, int xFrom, int xTo) {
        int result = 1;
        for (int i = xFrom; i < xTo; ++i) {
            result = 31 * result + hashCode(x[i]);
        }
        return result;
    }

    @Override
    public int hashCode(double[] x, int xFrom, int xTo) {
        int result = 1;
        for (int i = xFrom; i < xTo; ++i) {
            result = 31 * result + hashCode(x[i]);
        }
        return result;
    }

    @Override
    public int compare(float x, float y) {
        return Float.compare(x, y);
    }

    @Override
    public int compare(double x, double y) {
        return Double.compare(x, y);
    }

    @Override
    public int compare(float[] x, float[] y) {
        return Arrays.compare(x, y);
    }

    @Override
    public int compare(double[] x, double[] y) {
        return Arrays.compare(x, y);
    }

    @Override
    public int compare(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        return Arrays.compare(x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int compare(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        return Arrays.compare(x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int mismatch(float[] x, float[] y) {
        return Arrays.mismatch(x, y);
    }

    @Override
    public int mismatch(double[] x, double[] y) {
        return Arrays.mismatch(x, y);
    }

    @Override
    public int mismatch(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        return Arrays.mismatch(x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int mismatch(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        return Arrays.mismatch(x, xFrom, xTo, y, yFrom, yTo);
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
}
