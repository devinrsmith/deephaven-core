//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

public final class ArrayChunkEquals {

    // todo: need to consider float / double and DH treatment of them


    private static final BiPredicate<boolean[], boolean[]> BOOLEAN_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<char[], char[]> CHAR_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<byte[], byte[]> BYTE_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<short[], short[]> SHORT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<int[], int[]> INT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<long[], long[]> LONG_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<float[], float[]> FLOAT_ARRAY_EQUALS = ArrayChunkEquals::equals;
    private static final BiPredicate<double[], double[]> DOUBLE_ARRAY_EQUALS = ArrayChunkEquals::equals;

    private static final BiPredicate<Object[], Object[]> OBJECT_ARRAY_EQUALS = Arrays::equals;
    private static final BiPredicate<Object[], Object[]> OBJECT_ARRAY_DEEP_EQUALS = Arrays::deepEquals;

    private static final Map<Class<?>, BiPredicate<?, ?>> ARRAY_PREDICATES = Map.of(
            boolean[].class, BOOLEAN_ARRAY_EQUALS,
            char[].class, CHAR_ARRAY_EQUALS,
            byte[].class, BYTE_ARRAY_EQUALS,
            short[].class, SHORT_ARRAY_EQUALS,
            int[].class, INT_ARRAY_EQUALS,
            long[].class, LONG_ARRAY_EQUALS,
            float[].class, FLOAT_ARRAY_EQUALS,
            double[].class, DOUBLE_ARRAY_EQUALS,

            // this doesn't work for DH floats / doubles
            Object[].class, OBJECT_ARRAY_DEEP_EQUALS);


    private static final BiPredicate<Object, Object> OBJECT_EQUALS = Objects::equals;

    private static final ChunkEquals BOOLEAN_ARRAY_CHUNK_EQUALS =
            new ObjectChunkPredicateEquals<>(BOOLEAN_ARRAY_EQUALS);
    private static final ChunkEquals CHAR_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(CHAR_ARRAY_EQUALS);
    private static final ChunkEquals BYTE_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(BYTE_ARRAY_EQUALS);
    private static final ChunkEquals SHORT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(SHORT_ARRAY_EQUALS);
    private static final ChunkEquals INT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(INT_ARRAY_EQUALS);
    private static final ChunkEquals LONG_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(LONG_ARRAY_EQUALS);
    private static final ChunkEquals FLOAT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(FLOAT_ARRAY_EQUALS);
    private static final ChunkEquals DOUBLE_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(DOUBLE_ARRAY_EQUALS);
    private static final ChunkEquals OBJECT_ARRAY_CHUNK_EQUALS = new ObjectChunkPredicateEquals<>(OBJECT_ARRAY_EQUALS);
    private static final ChunkEquals OBJECT_ARRAY_CHUNK_DEEP_EQUALS =
            new ObjectChunkPredicateEquals<>(OBJECT_ARRAY_DEEP_EQUALS);

    public static ChunkEquals of() {
        return null;
    }

    public static boolean isPrimitiveLeaf(Class<?> arrayType) {
        if (!arrayType.isArray()) {
            throw new IllegalArgumentException();
        }
        return isPrimitiveLeafInternal(arrayType);
    }

    private static boolean isPrimitiveLeafInternal(Class<?> arrayType) {
        final Class<?> componentType = arrayType.getComponentType();
        if (componentType.isArray()) {
            return isPrimitiveLeafInternal(componentType);
        }
        return arrayType.isPrimitive();
    }

    public static <T> BiPredicate<T, T> equals(Class<T> clazz) {
        if (clazz.isPrimitive()) {
            throw new IllegalArgumentException();
        }
        if (clazz.isArray()) {
            // noinspection unchecked
            final BiPredicate<T, T> predicate = (BiPredicate<T, T>) ARRAY_PREDICATES.get(clazz);
            if (predicate != null) {
                return predicate;
            }
            // noinspection unchecked
            return (BiPredicate<T, T>) genericComponentArrayEquals(clazz.getComponentType());
        }
        return Objects::equals;
    }

    private static <T> BiPredicate<T[], T[]> genericComponentArrayEquals(Class<T> componentType) {
        if (componentType.isPrimitive()) {
            throw new IllegalArgumentException();
        }
        return new ArrayEquals<>(equals(componentType));
    }

    public static <T> boolean equals(T[] x, T[] y, BiPredicate<T, T> predicate) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        final int length = x.length;
        if (y.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!predicate.test(x[i], y[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(Object[] x, Object[] y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        final int length = x.length;
        if (y.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!deepEquals(x[i], y[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(float[] x, float[] y) {
        return Arrays.equals(x, y); // todo: this is wrong for DH
    }

    public static boolean equals(double[] x, double[] y) {
        return Arrays.equals(x, y); // todo: this is wrong for DH
    }

    static boolean deepEquals(Object e1, Object e2) {
        if (e1 instanceof Object[] && e2 instanceof Object[])
            return equals((Object[]) e1, (Object[]) e2);
        else if (e1 instanceof byte[] && e2 instanceof byte[])
            return Arrays.equals((byte[]) e1, (byte[]) e2);
        else if (e1 instanceof short[] && e2 instanceof short[])
            return Arrays.equals((short[]) e1, (short[]) e2);
        else if (e1 instanceof int[] && e2 instanceof int[])
            return Arrays.equals((int[]) e1, (int[]) e2);
        else if (e1 instanceof long[] && e2 instanceof long[])
            return Arrays.equals((long[]) e1, (long[]) e2);
        else if (e1 instanceof char[] && e2 instanceof char[])
            return Arrays.equals((char[]) e1, (char[]) e2);
        else if (e1 instanceof float[] && e2 instanceof float[])
            return equals((float[]) e1, (float[]) e2);
        else if (e1 instanceof double[] && e2 instanceof double[])
            return equals((double[]) e1, (double[]) e2);
        else if (e1 instanceof boolean[] && e2 instanceof boolean[])
            return Arrays.equals((boolean[]) e1, (boolean[]) e2);
        else
            return e1.equals(e2);
    }
}
