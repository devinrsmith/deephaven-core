//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.math;

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.Function;

public final class ArrayCopy<T> implements Function<T[], T[]> {

    private final Function<T, T> componentCopy;
    private final Class<T> componentType;

    public ArrayCopy(Function<T, T> componentCopy, Class<T> componentType) {
        this.componentCopy = Objects.requireNonNull(componentCopy);
        this.componentType = Objects.requireNonNull(componentType);
    }

    @Override
    public T[] apply(T[] ts) {
        if (ts == null) {
            return null;
        }
        // noinspection unchecked
        final T[] o = (T[]) Array.newInstance(componentType, ts.length);
        for (int i = 0; i < ts.length; ++i) {
            o[i] = componentCopy.apply(ts[i]);
        }
        return o;
    }
}
