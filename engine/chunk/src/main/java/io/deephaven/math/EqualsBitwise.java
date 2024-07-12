//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

final class EqualsBitwise extends EqualsDelegateBase {

    public static final EqualsBitwise INSTANCE = new EqualsBitwise();

    private EqualsBitwise() {}

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
}
