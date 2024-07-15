//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import java.util.Arrays;
import java.util.List;

public class ShortComparisons {

    public static int compare(short lhs, short rhs) {
        return Short.compare(lhs, rhs);
    }

    public static boolean eq(short lhs, short rhs) {
        return lhs == rhs;
    }

    public static int hashCode(short x) {
        return Short.hashCode(x);
    }

    public static boolean gt(short lhs, short rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(short lhs, short rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(short lhs, short rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(short lhs, short rhs) {
        return compare(lhs, rhs) <= 0;
    }

    public static boolean eq(short[] lhs, short[] rhs) {
        return Arrays.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for a {@code short[]} value consistent with {@link #eq(short[], short[])}; that is,
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
     * @return a hash code value for a {@code short[]} value.
     */
    public static int hashCode(short[] x) {
        return Arrays.hashCode(x);
    }
}
