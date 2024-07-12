//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import io.deephaven.base.SafeCloneable;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.function.Function;

public final class Copy {

    private static final Function<boolean[], boolean[]> BOOLEAN_ARRAY = boolean[]::clone;
    private static final Function<byte[], byte[]> BYTE_ARRAY = byte[]::clone;
    private static final Function<char[], char[]> CHAR_ARRAY = char[]::clone;
    private static final Function<short[], short[]> SHORT_ARRAY = short[]::clone;
    private static final Function<int[], int[]> INT_ARRAY = int[]::clone;
    private static final Function<long[], long[]> LONG_ARRAY = long[]::clone;
    private static final Function<float[], float[]> FLOAT_ARRAY = float[]::clone;
    private static final Function<double[], double[]> DOUBLE_ARRAY = double[]::clone;

    public static final Function<Object[], Object[]> SHALLOW_ARRAY_COPY = Copy::copyImpl;

    public static final Function<SafeCloneable<Object>, Object> SAFE_CLONE = SafeCloneable::safeClone;

    public static <T> Function<T, T> copy(Class<T> arrayType) {
        return copy(arrayType, false, null);
    }

    public static <T> Function<T, T> deepCopy(Class<T> arrayType) {
        return copy(arrayType, true, null);
    }

    public static <T, LT> Function<T, T> deepCopy(Class<T> arrayType, Function<LT, LT> leafF) {
        return copy(arrayType, true, leafF);
    }

    public static <T> Function<T, T> deepCopyShallowLeaf(Class<T> arrayType) {
        return copy(arrayType, true, Function.identity());
    }

    public static <T, LT extends SafeCloneable<LT>> Function<T, T> deepCopySafeCloneable(Class<T> arrayType) {
        if (!SafeCloneable.class.isAssignableFrom(getLeafComponent(arrayType))) {
            throw new IllegalArgumentException("leaf component must be SafeCloneable");
        }
        return deepCopy(arrayType, (Function<LT, LT>) SafeCloneable::safeClone);
    }

    private static Class<?> getLeafComponent(Class<?> clazz) {
        while (clazz.isArray()) {
            clazz = clazz.getComponentType();
        }
        return clazz;
    }

    private static <T> Function<T[], T[]> shallowCopy() {
        // noinspection unchecked,rawtypes
        return (Function<T[], T[]>) (Function) SHALLOW_ARRAY_COPY;
    }

    private static <T> T @Nullable [] copyImpl(T[] ts) {
        return ts == null ? null : Arrays.copyOf(ts, ts.length);
    }

    private static <T, LT> Function<T[], T[]> deepCopyImpl(Class<T> componentType, Function<LT, LT> leafF) {
        return new ArrayCopy<>(copy(componentType, true, leafF), componentType);
    }

    private static <T, LT> Function<T, T> copy(Class<T> arrayType, boolean deep, Function<LT, LT> leafF) {
        if (!arrayType.isArray()) {
            throw new IllegalArgumentException("Only array types are supported");
        }
        if (leafF != null) {
            if (!deep) {
                throw new IllegalArgumentException("leaf function only makes sense when deep");
            }
            if (getLeafComponent(arrayType).isPrimitive()) {
                throw new IllegalArgumentException("leaf function only makes sense with non-primitive leaf types");
            }
        }
        return copyImpl(arrayType, deep, leafF);
    }

    private static <T, LT> Function<T, T> copyImpl(Class<T> clazz, boolean deep, Function<LT, LT> leafF) {
        if (!clazz.isArray()) {
            if (leafF == null) {
                throw new IllegalArgumentException("Must supply leafFunction for non-primitive leafs");
            }
            // noinspection unchecked
            return (Function<T, T>) leafF;
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            if (deep) {
                // noinspection unchecked
                return (Function<T, T>) deepCopyImpl(clazz.getComponentType(), leafF);
            } else {
                // noinspection unchecked
                return (Function<T, T>) shallowCopy();
            }
        }
        if (boolean[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) BOOLEAN_ARRAY;
        }
        if (byte[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) BYTE_ARRAY;
        }
        if (char[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) CHAR_ARRAY;
        }
        if (short[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) SHORT_ARRAY;
        }
        if (int[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) INT_ARRAY;
        }
        if (long[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) LONG_ARRAY;
        }
        if (float[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) FLOAT_ARRAY;
        }
        if (double[].class == clazz) {
            // noinspection unchecked
            return (Function<T, T>) DOUBLE_ARRAY;
        }
        throw new IllegalStateException("Unexpected class: " + clazz);
    }
}
