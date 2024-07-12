//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.util.Objects;
import java.util.function.ToIntFunction;

public final class ArrayHasher<T> implements ToIntFunction<T[]> {

    private final ToIntFunction<T> elementHasher;

    public ArrayHasher(ToIntFunction<T> elementHasher) {
        this.elementHasher = Objects.requireNonNull(elementHasher);
    }

    @Override
    public int applyAsInt(T[] x) {
        if (x == null) {
            return 0;
        }
        int result = 1;
        for (T element : x) {
            result = 31 * result + elementHasher.applyAsInt(element);
        }
        return result;
    }
}
