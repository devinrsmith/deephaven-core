package io.deephaven.chunk.util.hashing;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

final class ConsistentMathImpl {
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

    static final Comparator<boolean[]> BOOLEAN_ARRAY_COMPARE = Arrays::compare;
    static final Comparator<byte[]> BYTE_ARRAY_COMPARE = Arrays::compare;
    static final Comparator<char[]> CHAR_ARRAY_COMPARE = Arrays::compare;
    static final Comparator<short[]> SHORT_ARRAY_COMPARE = Arrays::compare;
    static final Comparator<int[]> INT_ARRAY_COMPARE = Arrays::compare;
    static final Comparator<long[]> LONG_ARRAY_COMPARE = Arrays::compare;

    static <T> BiPredicate<T, T> predicate(ConsistentMath math, Class<T> clazz, boolean deep) {
        if (!clazz.isArray()) {
            if (clazz.isPrimitive()) {
                throw new IllegalArgumentException("Primitive types not supported");
            }
            if (deep && Objects.class == clazz) {
                // ie; new Object[] { 1, new int[] { 1, 2 }, new double[] { 3, 4 } }
                // also, would break for recursive:
                // Object[] x = new Object[1]
                // x[0] = x;
                throw new IllegalArgumentException(
                        "Should not set deep equals with Object leaf; would require runtime type and break for recursive types");
            }
            // noinspection unchecked
            return (BiPredicate<T, T>) OBJECTS_EQUALS;
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
            return (BiPredicate<T, T>) (BiPredicate<float[], float[]>) math::equals;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (BiPredicate<T, T>) (BiPredicate<double[], double[]>) math::equals;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            if (deep) {
                // noinspection unchecked
                return (BiPredicate<T, T>) new ArrayEquals<>(predicate(math, clazz.getComponentType(), deep));
            } else {
                // noinspection unchecked
                return (BiPredicate<T, T>) ARRAYS_EQUALS;
            }
        }
        throw new IllegalStateException();
    }

    static <T> ToIntFunction<T> hasher(ConsistentMath math, Class<T> clazz, boolean deep) {
        if (!clazz.isArray()) {
            if (clazz.isPrimitive()) {
                throw new IllegalArgumentException("Primitive types not supported");
            }
            if (deep && Objects.class == clazz) {
                // ie; new Object[] { 1, new int[] { 1, 2 }, new double[] { 3, 4 } }
                // also, would break for recursive:
                // Object[] x = new Object[1]
                // x[0] = x;
                throw new IllegalArgumentException(
                        "Should not set deep hashcode with Object leaf; would require runtime type and break for recursive types");
            }
            // noinspection unchecked
            return (ToIntFunction<T>) OBJECTS_HASHCODE;
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
            return (ToIntFunction<T>) (ToIntFunction<float[]>) math::hashCode;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (ToIntFunction<T>) (ToIntFunction<double[]>) math::hashCode;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            if (deep) {
                // noinspection unchecked
                return (ToIntFunction<T>) new ArrayHasher<>(hasher(math, clazz.getComponentType(), deep));
            } else {
                // noinspection unchecked
                return (ToIntFunction<T>) ARRAYS_HASHCODE;
            }
        }
        throw new IllegalStateException();
    }

    static <T> Comparator<T> comparator(ConsistentMath math, Class<T> clazz) {
        if (!clazz.isArray()) {
            if (clazz.isPrimitive()) {
                throw new IllegalArgumentException("Primitive types not supported");
            }
            if (!Comparable.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Leaf type must be Comparable");
            }
            //noinspection unchecked
            return (Comparator<T>) Comparator.naturalOrder();
        }
        if (boolean[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) BOOLEAN_ARRAY_COMPARE;
        }
        if (byte[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) BYTE_ARRAY_COMPARE;
        }
        if (char[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) CHAR_ARRAY_COMPARE;
        }
        if (short[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) SHORT_ARRAY_COMPARE;
        }
        if (int[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) INT_ARRAY_COMPARE;
        }
        if (long[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) LONG_ARRAY_COMPARE;
        }
        if (float[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) (Comparator<float[]>) math::compare;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (Comparator<T>) (Comparator<double[]>) math::compare;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            // noinspection unchecked
            return (Comparator<T>) new ArrayComparator<>(comparator(math, clazz.getComponentType()));
        }
        throw new IllegalStateException();
    }
}
