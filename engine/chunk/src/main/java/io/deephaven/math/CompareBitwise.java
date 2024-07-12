//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

final class CompareBitwise extends CompareBasicBase {

    public static final CompareBitwise INSTANCE = new CompareBitwise();

    private CompareBitwise() {
        super(Equals.bitwise());
    }

    @Override
    public int compare(float x, float y) {
        // this compare is consistent w/ hash / equals, but is not "natural"
        // could consider ieee total ordering
        return Integer.compare(Float.floatToRawIntBits(x), Float.floatToRawIntBits(y));
    }

    @Override
    public int compare(double x, double y) {
        // this compare is consistent w/ hash / equals, but is not "natural"
        // could consider ieee total ordering
        return Long.compare(Double.doubleToRawLongBits(x), Double.doubleToRawLongBits(y));
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
    public int compare(float[] x, int xFrom, int xTo, float[] y, int yFrom, int yTo) {
        return compare(this, x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public int compare(double[] x, int xFrom, int xTo, double[] y, int yFrom, int yTo) {
        return compare(this, x, xFrom, xTo, y, yFrom, yTo);
    }

    @Override
    public float min(float x, float y) {
        return compare(x, y) <= 0 ? x : y;
        // Note: intBitsToFloat may not preserve bit pattern
        // return Float.intBitsToFloat(java.lang.Math.min(Float.floatToRawIntBits(x), Float.floatToRawIntBits(y)));
    }

    @Override
    public double min(double x, double y) {
        return compare(x, y) <= 0 ? x : y;
        // Note: intBitsToDouble may not preserve bit pattern
        // return Double.intBitsToDouble(java.lang.Math.min(Double.doubleToRawIntBits(x),
        // Double.doubleToRawIntBits(y)));
    }

    @Override
    public float max(float x, float y) {
        return compare(x, y) >= 0 ? x : y;
        // Note: intBitsToFloat may not preserve bit pattern
        // return Float.intBitsToFloat(java.lang.Math.max(Float.floatToRawIntBits(x), Float.floatToRawIntBits(y)));
    }

    @Override
    public double max(double x, double y) {
        return compare(x, y) >= 0 ? x : y;
        // Note: intBitsToDouble may not preserve bit pattern
        // return Double.intBitsToDouble(java.lang.Math.max(Double.doubleToRawIntBits(x),
        // Double.doubleToRawIntBits(y)));
    }
}
