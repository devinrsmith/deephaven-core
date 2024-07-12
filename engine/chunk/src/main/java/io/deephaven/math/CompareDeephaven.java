//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;

final class CompareDeephaven extends CompareBase {
    public static final CompareDeephaven INSTANCE = new CompareDeephaven();

    private CompareDeephaven() {
        super(Math.deephaven());
    }

    @Override
    public int compare(char x, char y) {
        return CharComparisons.compare(x, y);
    }

    @Override
    public int compare(float x, float y) {
        return FloatComparisons.compare(x, y);
    }

    @Override
    public int compare(double x, double y) {
        return DoubleComparisons.compare(x, y);
    }

    @Override
    public int compare(char[] x, char[] y) {
        return compare(this, x, y);
    }

    @Override
    public int compare(float[] x, float[] y) {
        return compare(this, x, y);
    }

    @Override
    public int compare(double[] x, double[] y) {
        return compare(this, x, y);
    }

    @Override
    public int compare(char[] x, int xFrom, int xTo, char[] y, int yFrom, int yTo) {
        return compare(this, x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int compare(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        return compare(this, x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int compare(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        return compare(this, x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public char min(char x, char y) {
        return compare(x, y) <= 0 ? x : y;
    }

    @Override
    public float min(float x, float y) {
        return compare(x, y) <= 0 ? x : y;
    }

    @Override
    public double min(double x, double y) {
        return compare(x, y) <= 0 ? x : y;
    }

    @Override
    public char max(char x, char y) {
        return compare(x, y) >= 0 ? x : y;
    }

    @Override
    public float max(float x, float y) {
        return compare(x, y) >= 0 ? x : y;
    }

    @Override
    public double max(double x, double y) {
        return compare(x, y) >= 0 ? x : y;
    }
}
