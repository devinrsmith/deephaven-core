/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

public class LongComparisons {

    public static int compare(long lhs, long rhs) {
        return Long.compare(lhs, rhs);
    }

    public static long min(long lhs, long rhs) {
        return Math.min(lhs, rhs);
    }

    public static long max(long lhs, long rhs) {
        return Math.max(lhs, rhs);
    }

    public static boolean eq(long lhs, long rhs) {
        return lhs == rhs;
    }

    public static boolean gt(long lhs, long rhs) {
        return lhs > rhs;
    }

    public static boolean lt(long lhs, long rhs) {
        return lhs < rhs;
    }

    public static boolean geq(long lhs, long rhs) {
        return lhs >= rhs;
    }

    public static boolean leq(long lhs, long rhs) {
        return lhs <= rhs;
    }
}
