//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

abstract class EqualsBase implements Equals {
    static final BiPredicate<boolean[], boolean[]> BOOLEAN_ARRAY_EQUALS = Arrays::equals;
    static final BiPredicate<byte[], byte[]> BYTE_ARRAY_EQUALS = Arrays::equals;
    static final BiPredicate<char[], char[]> CHAR_ARRAY_EQUALS = Arrays::equals;
    static final BiPredicate<short[], short[]> SHORT_ARRAY_EQUALS = Arrays::equals;
    static final BiPredicate<int[], int[]> INT_ARRAY_EQUALS = Arrays::equals;
    static final BiPredicate<long[], long[]> LONG_ARRAY_EQUALS = Arrays::equals;
    static final BiPredicate<Object, Object> OBJECTS_EQUALS = Objects::equals;
    static final BiPredicate<Object[], Object[]> ARRAYS_EQUALS = Arrays::equals;

    static final ToIntFunction<boolean[]> BOOLEAN_ARRAY_HASHCODE = Arrays::hashCode;
    static final ToIntFunction<byte[]> BYTE_ARRAY_HASHCODE = Arrays::hashCode;
    static final ToIntFunction<char[]> CHAR_ARRAY_HASHCODE = Arrays::hashCode;
    static final ToIntFunction<short[]> SHORT_ARRAY_HASHCODE = Arrays::hashCode;
    static final ToIntFunction<int[]> INT_ARRAY_HASHCODE = Arrays::hashCode;
    static final ToIntFunction<long[]> LONG_ARRAY_HASHCODE = Arrays::hashCode;
    static final ToIntFunction<Object> OBJECTS_HASHCODE = Objects::hashCode;
    static final ToIntFunction<Object[]> ARRAYS_HASHCODE = Arrays::hashCode;

    private static final Function<boolean[], boolean[]> BOOLEAN_ARRAY_CLONE = boolean[]::clone;

    private final BiPredicate<float[], float[]> equalsFloatArray;
    private final BiPredicate<double[], double[]> equalsDoubleArray;

    private final ToIntFunction<float[]> hasherFloatArray;
    private final ToIntFunction<double[]> hasherDoubleArray;

    EqualsBase() {
        equalsFloatArray = this::equals;
        equalsDoubleArray = this::equals;
        hasherFloatArray = this::hashCode;
        hasherDoubleArray = this::hashCode;
    }

    @Override
    public final <T> BiPredicate<T, T> equals(Class<T> clazz) {
        return predicate(clazz, false);
    }

    @Override
    public final <T> BiPredicate<T, T> deepEquals(Class<T> clazz) {
        return predicate(clazz, true);
    }

    @Override
    public final <T> ToIntFunction<T> hashCode(Class<T> clazz) {
        return hasher(clazz, false);
    }

    @Override
    public final <T> ToIntFunction<T> deepHashCode(Class<T> clazz) {
        return hasher(clazz, true);
    }

    private <T> BiPredicate<T, T> predicate(Class<T> clazz, boolean deep) {
        if (!clazz.isArray()) {
            if (clazz.isPrimitive()) {
                throw new IllegalArgumentException("Primitive types not supported: " + clazz);
            }
            if (deep && Objects.class == clazz) {
                // ie; new Object[] { 1, new int[] { 1, 2 }, new double[] { 3, 4 } }
                // also, would break for recursive:
                // Object[] x = new Object[1]
                // x[0] = x;
                throw new IllegalArgumentException(
                        "Should not set deep equals with Object leaf; would require runtime type check and break for recursive types");
            }
            // noinspection unchecked
            return (BiPredicate<T, T>) OBJECTS_EQUALS;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            if (deep) {
                // noinspection unchecked
                return (BiPredicate<T, T>) new ArrayEquals<>(predicate(clazz.getComponentType(), deep));
            } else {
                // noinspection unchecked
                return (BiPredicate<T, T>) ARRAYS_EQUALS;
            }
        }
        if (boolean[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) BOOLEAN_ARRAY_EQUALS;
        }
        if (byte[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) BYTE_ARRAY_EQUALS;
        }
        if (char[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) CHAR_ARRAY_EQUALS;
        }
        if (short[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) SHORT_ARRAY_EQUALS;
        }
        if (int[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) INT_ARRAY_EQUALS;
        }
        if (long[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) LONG_ARRAY_EQUALS;
        }
        if (float[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) equalsFloatArray;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) equalsDoubleArray;
        }
        throw new IllegalStateException("Unexpected class: " + clazz);
    }

    private <T> ToIntFunction<T> hasher(Class<T> clazz, boolean deep) {
        if (!clazz.isArray()) {
            if (clazz.isPrimitive()) {
                throw new IllegalArgumentException("Primitive types not supported: " + clazz);
            }
            if (deep && Objects.class == clazz) {
                // ie; new Object[] { 1, new int[] { 1, 2 }, new double[] { 3, 4 } }
                // also, would break for recursive:
                // Object[] x = new Object[1]
                // x[0] = x;
                throw new IllegalArgumentException(
                        "Should not set deep hashcode with Object leaf; would require runtime type check and break for recursive types");
            }
            // noinspection unchecked
            return (ToIntFunction<T>) OBJECTS_HASHCODE;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            if (deep) {
                // noinspection unchecked
                return (ToIntFunction<T>) new ArrayHasher<>(hasher(clazz.getComponentType(), deep));
            } else {
                // noinspection unchecked
                return (ToIntFunction<T>) ARRAYS_HASHCODE;
            }
        }
        if (boolean[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) BOOLEAN_ARRAY_HASHCODE;
        }
        if (byte[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) BYTE_ARRAY_HASHCODE;
        }
        if (char[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) CHAR_ARRAY_HASHCODE;
        }
        if (short[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) SHORT_ARRAY_HASHCODE;
        }
        if (int[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) INT_ARRAY_HASHCODE;
        }
        if (long[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) LONG_ARRAY_HASHCODE;
        }
        if (float[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) hasherFloatArray;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) hasherDoubleArray;
        }
        throw new IllegalStateException("Unexpected class: " + clazz);
    }

    static int hashCode(Equals math, float[] x, int xFrom, int xTo) {
        int result = 1;
        for (int i = xFrom; i < xTo; ++i) {
            result = 31 * result + math.hashCode(x[i]);
        }
        return result;
    }

    static int hashCode(Equals math, double[] x, int xFrom, int xTo) {
        int result = 1;
        for (int i = xFrom; i < xTo; ++i) {
            result = 31 * result + math.hashCode(x[i]);
        }
        return result;
    }
}
