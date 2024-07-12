//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;

// NaNs are all equal
// -0.0 / 0.0 are equal
final class ConsistentMathDh extends ConsistentMathBase {
    public static final ConsistentMathDh INSTANCE = new ConsistentMathDh();

    private ConsistentMathDh() {}

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
        return Double.hashCode(x == -0.0 ? 0.0 : x);
    }

    @Override
    public int compare(float x, float y) {
        // expensive; we could have a cheaper one if we didn't care about "natural" looking order
        return FloatComparisons.compare(x, y);
    }

    @Override
    public int compare(double x, double y) {
        // expensive; we could have a cheaper one if we didn't care about "natural" looking order
        return DoubleComparisons.compare(x, y);
    }
}
