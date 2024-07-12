//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public class ArrayWhat<T> {

    private static final ArrayWhat<Object> IDENTITY =
            new ArrayWhat<>(System::identityHashCode, (x, y) -> x == y, Function.identity());
    private static final ArrayWhat<int[]> INT = new ArrayWhat<>(Arrays::hashCode, Arrays::equals, int[]::clone);
    private static final ArrayWhat<Object[]> OBJ = new ArrayWhat<>(Arrays::hashCode, Arrays::equals, Object[]::clone);

    // todo: this doesn't work w/ float / double depending on NaN, -0, 0 concerns.
    private static final ArrayWhat<Object[]> DEEP_OBJ =
            new ArrayWhat<>(Arrays::deepHashCode, Arrays::deepEquals, ArrayWhat::deepArrayCopy);

    private final ToIntFunction<T> hash;
    private final BiPredicate<T, T> equals;
    private final Function<T, T> copy;

    public ArrayWhat(ToIntFunction<T> hash, BiPredicate<T, T> equals, Function<T, T> copy) {
        this.hash = hash;
        this.equals = equals;
        this.copy = copy;
    }

    private static <T> T deepArrayCopy(T array) {
        if (array == null) {
            return null;
        }
        final int length = Array.getLength(array);
        final Class<?> componentType = array.getClass().getComponentType();
        // noinspection unchecked
        final T dst = (T) Array.newInstance(componentType, length);
        if (componentType.isArray()) {
            for (int i = 0; i < length; ++i) {
                Array.set(dst, i, deepArrayCopy(Array.get(array, i)));
            }
        } else {
            System.arraycopy(array, 0, dst, 0, length);
        }
        return dst;
    }
}
