//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import java.util.Arrays;
import java.util.List;

public class IntComparisons {

    public static int compare(int lhs, int rhs) {
        return Integer.compare(lhs, rhs);
    }

    public static boolean eq(int lhs, int rhs) {
        return lhs == rhs;
    }

    public static int hashCode(int x) {
        return Integer.hashCode(x);
    }

    public static boolean gt(int lhs, int rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(int lhs, int rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(int lhs, int rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(int lhs, int rhs) {
        return compare(lhs, rhs) <= 0;
    }

    public static boolean eq(int[] lhs, int[] rhs) {
        return Arrays.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for a {@code int[]} value consistent with {@link #eq(int[], int[])}; that is,
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
     * @return a hash code value for a {@code int[]} value.
     */
    public static int hashCode(int[] x) {
        return Arrays.hashCode(x);
    }
}
