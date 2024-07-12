//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Arrays;

final class CompareJava extends CompareBasicBase {

    public static final CompareJava INSTANCE = new CompareJava();

    private CompareJava() {
        super(Math.java());
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
    public float min(float x, float y) {
        // Can't use java.lang.Math.min as it is NaN viral
        return compare(x, y) <= 0 ? x : y;
    }

    @Override
    public double min(double x, double y) {
        // Can't use java.lang.Math.min as it is NaN viral
        return compare(x, y) <= 0 ? x : y;
    }

    @Override
    public float max(float x, float y) {
        // NaN viral, which is okay b/c Float#compare treats NaN as max
        // Also, treats -0.0f < 0.0f which is what we need.
        return java.lang.Math.max(x, y);
    }

    @Override
    public double max(double x, double y) {
        // NaN viral, which is okay b/c Double#compare treats NaN as max
        // Also, treats -0.0 < 0.0 which is what we need.
        return java.lang.Math.max(x, y);
    }
}
