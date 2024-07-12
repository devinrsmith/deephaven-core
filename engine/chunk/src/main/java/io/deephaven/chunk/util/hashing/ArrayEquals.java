//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.hashing;

import java.util.Objects;
import java.util.function.BiPredicate;

final class ArrayEquals<T> implements BiPredicate<T[], T[]> {

    private final BiPredicate<T, T> componentPredicate;

    public ArrayEquals(BiPredicate<T, T> componentPredicate) {
        this.componentPredicate = Objects.requireNonNull(componentPredicate);
    }

    @Override
    public boolean test(T[] x, T[] y) {
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
            if (!componentPredicate.test(x[i], y[i])) {
                return false;
            }
        }
        return true;
    }
}
