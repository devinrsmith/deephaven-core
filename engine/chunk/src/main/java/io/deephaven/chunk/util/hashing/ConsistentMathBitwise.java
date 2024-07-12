//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

// specific NaNs are equal
// -0.0 / 0.0 are not equal
final class ConsistentMathBitwise extends ConsistentMathBase {

    public static final ConsistentMathBitwise INSTANCE = new ConsistentMathBitwise();

    private ConsistentMathBitwise() {}

    @Override
    public boolean equals(float x, float y) {
        return Float.floatToRawIntBits(x) == Float.floatToRawIntBits(y);
    }

    @Override
    public boolean equals(double x, double y) {
        return Double.doubleToRawLongBits(x) == Double.doubleToRawLongBits(y);
    }

    @Override
    public int hashCode(float x) {
        return Float.floatToRawIntBits(x);
    }

    @Override
    public int hashCode(double x) {
        return Long.hashCode(Double.doubleToRawLongBits(x));
    }

    @Override
    public int compare(float x, float y) {
        // this compare is consistent w/ hash / equals, but is not "natural"
        return Integer.compare(Float.floatToRawIntBits(x), Float.floatToRawIntBits(y));
    }

    @Override
    public int compare(double x, double y) {
        // this compare is consistent w/ hash / equals, but is not "natural"
        return Long.compare(Double.doubleToRawLongBits(x), Double.doubleToRawLongBits(y));
    }

    // note: equals(float[], float[]) & equals(double[], double[]) implementations could likely be improved with access
    // to Unsafe.
}
