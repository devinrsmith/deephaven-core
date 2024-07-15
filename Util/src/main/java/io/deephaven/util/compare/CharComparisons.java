//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.List;

public class CharComparisons {

    /**
     * Compares two chars according to the following rules:
     *
     * <ul>
     * <li>{@link QueryConstants#NULL_CHAR} is less than all other {@code char} values</li>
     * <li>Otherwise, normal {@code char} comparison logic is used</li>
     * </ul>
     *
     * <p>
     * Note: this differs from the Java language numerical comparison operators {@code <, <=, >=, >} and
     * {@link Character#compare(char, char)}.
     *
     * @param lhs the first {@code char} to compare
     * @param rhs the second {@code char} to compare
     * @return the value {@code 0} if {@code d1} is "equal" to {@code d2}; a value less than {@code 0} if {@code d1} is
     *         "less than" {@code d2}; and a value greater than {@code 0} if {@code d1} is "greater than" {@code d2}.
     */
    public static int compare(char lhs, char rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == QueryConstants.NULL_CHAR) {
            return -1;
        }
        if (rhs == QueryConstants.NULL_CHAR) {
            return 1;
        }
        return Character.compare(lhs, rhs);
    }

    /**
     * Tests two chars for equality consistent with {@link #compare(char, char)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * @param lhs the first {@code char} to test
     * @param rhs the second {@code char} to test
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(char lhs, char rhs) {
        return lhs == rhs;
    }

    /**
     * Returns a hash code for a {@code char} value consistent with {@link #eq(char, char)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code char} value.
     */
    public static int hashCode(char x) {
        return Character.hashCode(x);
    }

    public static boolean gt(char lhs, char rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(char lhs, char rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(char lhs, char rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(char lhs, char rhs) {
        return compare(lhs, rhs) <= 0;
    }

    /**
     * Returns {@code true} if the two specified arrays of chars are <i>equal</i> to one another. Two arrays are
     * considered equal if both arrays contain the same number of elements, and all corresponding pairs of elements in
     * the two arrays are {@link #eq(char, char)}. Also, two array references are considered equal if both are
     * {@code null}.
     *
     * @param lhs one array to be tested for equality
     * @param rhs the other array to be tested for equality
     * @return {@code true} if the two arrays are equal
     */
    public static boolean eq(char[] lhs, char[] rhs) {
        return Arrays.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for a {@code char[]} value consistent with {@link #eq(char[], char[])}; that is,
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
     * @return a hash code value for a {@code char[]} value.
     */
    public static int hashCode(char[] x) {
        return Arrays.hashCode(x);
    }
}
