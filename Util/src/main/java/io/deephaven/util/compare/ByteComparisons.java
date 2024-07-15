//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import java.util.Arrays;
import java.util.List;

public class ByteComparisons {

    public static int compare(byte lhs, byte rhs) {
        return Byte.compare(lhs, rhs);
    }

    public static boolean eq(byte lhs, byte rhs) {
        return lhs == rhs;
    }

    public static int hashCode(byte x) {
        return Byte.hashCode(x);
    }

    public static boolean gt(byte lhs, byte rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(byte lhs, byte rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(byte lhs, byte rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(byte lhs, byte rhs) {
        return compare(lhs, rhs) <= 0;
    }

    public static boolean eq(byte[] lhs, byte[] rhs) {
        return Arrays.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for a {@code byte[]} value consistent with {@link #eq(byte[], byte[])}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}. Furthermore, this follows the {@link List#hashCode()} convention
     *
     * <pre>{@code
     * int hashCode = 1;
     * for (E e : array)
     *     hashCode = 31 * hashCode + hashCode(e);
     * }</pre>
     *
     * If {@code x} is {@code null}, this method returns 0.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code byte[]} value.
     */
    public static int hashCode(byte[] x) {
        return Arrays.hashCode(x);
    }
}
