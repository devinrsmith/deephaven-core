/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

public class ShortComparisons {

    public static int compare(short lhs, short rhs) {
        return Short.compare(lhs, rhs);
    }

    public static short min(short lhs, short rhs) {
        return (short) Math.min(lhs, rhs);
    }

    public static short max(short lhs, short rhs) {
        return (short) Math.max(lhs, rhs);
    }

    public static boolean eq(short lhs, short rhs) {
        return lhs == rhs;
    }

    public static boolean gt(short lhs, short rhs) {
        return lhs > rhs;
    }

    public static boolean lt(short lhs, short rhs) {
        return lhs < rhs;
    }

    public static boolean geq(short lhs, short rhs) {
        return lhs >= rhs;
    }

    public static boolean leq(short lhs, short rhs) {
        return lhs <= rhs;
    }
}
