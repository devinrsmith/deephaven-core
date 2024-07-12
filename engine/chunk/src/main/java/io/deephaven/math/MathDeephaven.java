//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;

final class MathDeephaven extends MathDelegateBase {

    public static final MathDeephaven INSTANCE = new MathDeephaven();

    private MathDeephaven() {}

    @Override
    public boolean equals(float x, float y) {
        return FloatComparisons.eq(x, y);
    }

    @Override
    public boolean equals(double x, double y) {
        return DoubleComparisons.eq(x, y);
    }

    @Override
    public int hashCode(float x) {
        // todo: compare speed w/ branching on outside
        return Float.hashCode(x == -0.0f ? 0.0f : x);
    }

    @Override
    public int hashCode(double x) {
        // todo: compare speed w/ branching on outside
        return Double.hashCode(x == -0.0d ? 0.0d : x);
    }
}
