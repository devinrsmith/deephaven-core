//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ObjectComparisons {

    public static int compare(Object lhs, Object rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        // noinspection unchecked,rawtypes
        return ((Comparable) lhs).compareTo(rhs);
    }

    public static boolean eq(Object lhs, Object rhs) {
        return Objects.equals(lhs, rhs);
    }

    public static int hashCode(Object x) {
        return Objects.hashCode(x);
    }

    public static boolean gt(Object lhs, Object rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(Object lhs, Object rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(Object lhs, Object rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(Object lhs, Object rhs) {
        return compare(lhs, rhs) <= 0;
    }

    public static boolean eq(Object[] lhs, Object[] rhs) {
        return Arrays.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for a {@code Object[]} value consistent with {@link #eq(Object[], Object[])}; that is,
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
     * @return a hash code value for a {@code Object[]} value.
     */
    public static int hashCode(Object[] x) {
        return Arrays.hashCode(x);
    }
}
