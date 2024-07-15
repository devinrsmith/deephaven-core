//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

import java.util.List;

public class FloatComparisons {

    private static final int ZERO_HASHCODE = Float.hashCode(0.0f);

    /**
     * Compares two floats according to the following rules:
     *
     * <ul>
     * <li>{@link QueryConstants#NULL_FLOAT} is less than all other {@code float} values (including
     * {@link Float#NEGATIVE_INFINITY})</li>
     * <li>{@code 0.0} and {@code -0.0} are equal</li>
     * <li>{@link Float#NaN} (and all other float {@code NaN} representations) is equal to {@link Float#NaN} and greater
     * than all other {@code float} values (including {@link Float#POSITIVE_INFINITY})</li>
     * <li>Otherwise, normal {@code float} comparison logic is used</li>
     * </ul>
     *
     * <p>
     * Note: this differs from the Java language numerical comparison operators {@code <, <=, ==, >=, >} and
     * {@link Float#compare(float, float)}.
     *
     * @param lhs the first {@code float} to compare
     * @param rhs the second {@code float} to compare
     * @return the value {@code 0} if {@code d1} is "equal" to {@code d2}; a value less than {@code 0} if {@code d1} is
     *         "less than" {@code d2}; and a value greater than {@code 0} if {@code d1} is "greater than" {@code d2}.
     */
    public static int compare(float lhs, float rhs) {
        // Note this intentionally makes -0.0 and 0.0 compare equal
        if (lhs == rhs) {
            return 0;
        }
        // One could be NULL, but not both
        if (lhs == QueryConstants.NULL_FLOAT) {
            return -1;
        }
        if (rhs == QueryConstants.NULL_FLOAT) {
            return 1;
        }
        // One or both could be NaN
        if (Float.isNaN(lhs)) {
            if (Float.isNaN(rhs)) {
                return 0; // Both NaN
            }
            return 1; // lhs is NaN, rhs is not
        }
        if (Float.isNaN(rhs)) {
            return -1; // rhs is NaN, lhs is not
        }
        // Neither is NULL or NaN, and they are not equal; fall back to regular comparisons
        return lhs < rhs ? -1 : 1;
    }

    /**
     * Tests two floats for equality consistent with {@link #compare(float, float)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Note: this differs from the Java language numerical equality operators {@code ==} and
     * {@link Float#equals(Object)}.
     *
     * @param lhs the first {@code float} to test
     * @param rhs the second {@code float} to test
     * @return {@code true} if the values are "equal", {@code false} otherwise
     */
    public static boolean eq(float lhs, float rhs) {
        return (Float.isNaN(lhs) && Float.isNaN(rhs)) || lhs == rhs;
    }

    /**
     * Returns a hash code for a {@code float} value consistent with {@link #eq(float, float)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * <p>
     * Note: this differs from {@link Float#hashCode(float)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code float} value.
     */
    public static int hashCode(float x) {
        // Note this intentionally makes -0.0f and 0.0f hashcode equal
        return x == 0.0f
                ? ZERO_HASHCODE
                : Float.hashCode(x);
    }

    public static boolean gt(float lhs, float rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(float lhs, float rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(float lhs, float rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(float lhs, float rhs) {
        return compare(lhs, rhs) <= 0;
    }

    /**
     * Returns {@code true} if the two specified arrays of floats are <i>equal</i> to one another. Two arrays are
     * considered equal if both arrays contain the same number of elements, and all corresponding pairs of elements in
     * the two arrays are {@link #eq(float, float)}. Also, two array references are considered equal if both are
     * {@code null}.
     *
     * <p>
     * Note: this differs from {@link java.util.Arrays#equals(float[], float[])}.
     *
     * @param lhs one array to be tested for equality
     * @param rhs the other array to be tested for equality
     * @return {@code true} if the two arrays are equal
     */
    public static boolean eq(float[] lhs, float[] rhs) {
        if (lhs == rhs) {
            return true;
        }
        if (lhs == null || rhs == null) {
            return false;
        }
        final int length = lhs.length;
        if (rhs.length != length) {
            return false;
        }
        for (int i = 0; i < length; ++i) {
            if (!eq(lhs[i], rhs[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a hash code for a {@code float[]} value consistent with {@link #eq(float[], float[])}; that is,
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
     * @return a hash code value for a {@code float[]} value.
     */
    public static int hashCode(float[] x) {
        if (x == null) {
            return 0;
        }
        int result = 1;
        for (float element : x) {
            result = 31 * result + hashCode(element);
        }
        return result;
    }
}
