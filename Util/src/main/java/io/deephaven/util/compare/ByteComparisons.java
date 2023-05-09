/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

public class ByteComparisons {

    public static int compare(byte lhs, byte rhs) {
        return Byte.compare(lhs, rhs);
    }

    public static byte min(byte lhs, byte rhs) {
        return (byte) Math.min(lhs, rhs);
    }

    public static byte max(byte lhs, byte rhs) {
        return (byte) Math.max(lhs, rhs);
    }

    public static boolean eq(byte lhs, byte rhs) {
        return lhs == rhs;
    }

    public static boolean gt(byte lhs, byte rhs) {
        return lhs > rhs;
    }

    public static boolean lt(byte lhs, byte rhs) {
        return lhs < rhs;
    }

    public static boolean geq(byte lhs, byte rhs) {
        return lhs >= rhs;
    }

    public static boolean leq(byte lhs, byte rhs) {
        return lhs <= rhs;
    }
}
