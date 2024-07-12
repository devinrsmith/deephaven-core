//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Arrays;

final class MathJava extends MathBase {

    public static final MathJava INSTANCE = new MathJava();

    private MathJava() {}

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
        return hashCode(this, x, xFrom, xTo);
    }

    @Override
    public int hashCode(double[] x, int xFrom, int xTo) {
        return hashCode(this, x, xFrom, xTo);
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
}
