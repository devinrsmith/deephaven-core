/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.function.DoublePredicate;

public class DoubleComparisons {

    public static int compare(double lhs, double rhs) {
        // Note this intentionally makes -0.0 and 0.0 compare equal
        if (lhs == rhs) {
            return 0;
        }
        // One could be NULL, but not both
        if (lhs == QueryConstants.NULL_DOUBLE) {
            return -1;
        }
        if (rhs == QueryConstants.NULL_DOUBLE) {
            return 1;
        }
        // One or both could be NaN
        if (Double.isNaN(lhs)) {
            if (Double.isNaN(rhs)) {
                return 0; // Both NaN
            }
            return 1; // lhs is NaN, rhs is not
        }
        if (Double.isNaN(rhs)) {
            return -1; // rhs is NaN, lhs is not
        }
        // Neither is NULL or NaN, and they are not equal; fall back to regular comparisons
        return lhs < rhs ? -1 : 1;
    }

    @SuppressWarnings("UseCompareMethod")
    private static int compareRhsNotNullNorNaN(double lhs, double rhs) {
        if (lhs == QueryConstants.NULL_DOUBLE) {
            // nulls first
            return -1;
        }
        if (Double.isNaN(lhs)) {
            // NaNs last
            return 1;
        }
        if (lhs < rhs) {
            return -1;
        }
        if (lhs > rhs) {
            return 1;
        }
        // Note this intentionally makes -0.0 and 0.0 compare equal, which is the behavior of -0.0 == 0.0
        return 0;
        // If we need to eventually distinguish between -0.0 and 0.0:
        // final long lhsBits = Double.doubleToRawLongBits(lhs);
        // final long rhsBits = Double.doubleToRawLongBits(rhs);
        // // noinspection UseCompareMethod
        // return lhsBits == rhsBits ? 0 : (lhsBits < rhsBits ? -1 : 1);
    }

    public static boolean eq(double lhs, double rhs) {
        return (Double.isNaN(lhs) && Double.isNaN(rhs)) || lhs == rhs;
    }

    public static boolean gt(double lhs, double rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(double lhs, double rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(double lhs, double rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(double lhs, double rhs) {
        return compare(lhs, rhs) <= 0;
    }

    public static DoublePredicate geq(double pivot) {
        return pivot == QueryConstants.NULL_DOUBLE
                ? True.INSTANCE
                : (Double.isNaN(pivot)
                        ? IsNaN.INSTANCE
                        : new GEQ(pivot));
    }

    public static DoublePredicate gt(double pivot) {
        return Double.isNaN(pivot)
                ? False.INSTANCE
                : (pivot == QueryConstants.NULL_DOUBLE
                        ? IsNull.INSTANCE.negate()
                        : new GT(pivot));
    }

    public static DoublePredicate leq(double pivot) {
        return Double.isNaN(pivot)
                ? True.INSTANCE
                : (pivot == QueryConstants.NULL_DOUBLE
                        ? IsNull.INSTANCE
                        : new LEQ(pivot));
    }

    public static DoublePredicate lt(double pivot) {
        return pivot == QueryConstants.NULL_DOUBLE
                ? False.INSTANCE
                : (Double.isNaN(pivot)
                        ? IsNaN.INSTANCE.negate()
                        : new LT(pivot));
    }

    public static DoublePredicate between(
            double lower,
            double upper,
            boolean lowerInclusive,
            boolean upperInclusive) {
        if (!leq(lower, upper)) {
            throw new IllegalArgumentException();
        }
        final DoublePredicate lowerPredicate = lowerInclusive
                ? DoubleComparisons.geq(lower)
                : DoubleComparisons.gt(lower);
        final DoublePredicate upperPredicate = upperInclusive
                ? DoubleComparisons.leq(upper)
                : DoubleComparisons.lt(upper);
        return and(lowerPredicate, upperPredicate);
    }

    public static DoublePredicate or(DoublePredicate x, DoublePredicate y) {
        if (isTrue(y)) {
            return y.or(x);
        }
        if (isFalse(y)) {
            return y.or(x);
        }
        return x.or(y);
    }

    public static DoublePredicate and(DoublePredicate x, DoublePredicate y) {
        if (isTrue(y)) {
            return y.and(x);
        }
        if (isFalse(y)) {
            return y.and(x);
        }
        return x.and(y);
    }

    public static boolean isTrue(DoublePredicate p) {
        return p == True.INSTANCE;
    }

    public static boolean isFalse(DoublePredicate p) {
        return p == False.INSTANCE;
    }

    private enum True implements DoublePredicate {
        INSTANCE;

        @Override
        public boolean test(double value) {
            return true;
        }

        @NotNull
        @Override
        public DoublePredicate and(@NotNull DoublePredicate other) {
            return other;
        }

        @NotNull
        @Override
        public DoublePredicate negate() {
            return False.INSTANCE;
        }

        @NotNull
        @Override
        public DoublePredicate or(@NotNull DoublePredicate other) {
            return this;
        }
    }

    private enum False implements DoublePredicate {
        INSTANCE;

        @Override
        public boolean test(double value) {
            return false;
        }

        @NotNull
        @Override
        public DoublePredicate and(@NotNull DoublePredicate other) {
            return this;
        }

        @NotNull
        @Override
        public DoublePredicate negate() {
            return True.INSTANCE;
        }

        @NotNull
        @Override
        public DoublePredicate or(@NotNull DoublePredicate other) {
            return other;
        }
    }

    private enum IsNaN implements DoublePredicate {
        INSTANCE;

        @Override
        public boolean test(double value) {
            return Double.isNaN(value);
        }
    }

    private enum IsNull implements DoublePredicate {
        INSTANCE;

        @Override
        public boolean test(double value) {
            return value == QueryConstants.NULL_DOUBLE;
        }
    }

    private static class GEQ implements DoublePredicate {
        private final double pivot;

        private GEQ(double pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean test(double value) {
            return DoubleComparisons.compareRhsNotNullNorNaN(value, pivot) >= 0;
        }

        @NotNull
        @Override
        public DoublePredicate negate() {
            return new LT(pivot);
        }
    }

    private static class GT implements DoublePredicate {
        private final double pivot;

        private GT(double pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean test(double value) {
            return DoubleComparisons.compareRhsNotNullNorNaN(value, pivot) > 0;
        }

        @NotNull
        @Override
        public DoublePredicate negate() {
            return new LEQ(pivot);
        }
    }

    private static class LEQ implements DoublePredicate {
        private final double pivot;

        private LEQ(double pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean test(double value) {
            return DoubleComparisons.compareRhsNotNullNorNaN(value, pivot) <= 0;
        }

        @NotNull
        @Override
        public DoublePredicate negate() {
            return new GT(pivot);
        }
    }

    private static class LT implements DoublePredicate {
        private final double pivot;

        private LT(double pivot) {
            this.pivot = pivot;
        }

        @Override
        public boolean test(double value) {
            return DoubleComparisons.compareRhsNotNullNorNaN(value, pivot) < 0;
        }

        @NotNull
        @Override
        public DoublePredicate negate() {
            return new GEQ(pivot);
        }
    }
}
