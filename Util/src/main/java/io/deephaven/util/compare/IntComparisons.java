/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

public class IntComparisons {

    public static int compare(int lhs, int rhs) {
        return Integer.compare(lhs, rhs);
    }

    public static int min(int lhs, int rhs) {
        return Math.min(lhs, rhs);
    }

    public static int max(int lhs, int rhs) {
        return Math.max(lhs, rhs);
    }

    public static boolean eq(int lhs, int rhs) {
        return lhs == rhs;
    }

    public static boolean gt(int lhs, int rhs) {
        return lhs > rhs;
    }

    public static boolean lt(int lhs, int rhs) {
        return lhs < rhs;
    }

    public static boolean geq(int lhs, int rhs) {
        return lhs >= rhs;
    }

    public static boolean leq(int lhs, int rhs) {
        return lhs <= rhs;
    }
}
