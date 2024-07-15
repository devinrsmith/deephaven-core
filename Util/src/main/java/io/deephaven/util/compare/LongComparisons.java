//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import java.util.Arrays;
import java.util.List;

public class LongComparisons {

    public static int compare(long lhs, long rhs) {
        return Long.compare(lhs, rhs);
    }

    public static boolean eq(long lhs, long rhs) {
        return lhs == rhs;
    }

    public static int hashCode(long x) {
        return Long.hashCode(x);
    }

    public static boolean gt(long lhs, long rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(long lhs, long rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(long lhs, long rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(long lhs, long rhs) {
        return compare(lhs, rhs) <= 0;
    }

    public static boolean eq(long[] lhs, long[] rhs) {
        return Arrays.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for a {@code long[]} value consistent with {@link #eq(long[], long[])}; that is,
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
     * @return a hash code value for a {@code long[]} value.
     */
    public static int hashCode(long[] x) {
        return Arrays.hashCode(x);
    }
}
