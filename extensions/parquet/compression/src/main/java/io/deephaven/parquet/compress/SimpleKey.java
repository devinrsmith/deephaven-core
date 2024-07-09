//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import io.deephaven.parquet.compress.CompressorAdapter.Cache.Key;

import java.util.Objects;

final class SimpleKey<T> implements Key<T> {
    private final String name;

    public SimpleKey(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public String toString() {
        return "SimpleKey{" +
                "name='" + name + '\'' +
                ", identity=" + System.identityHashCode(this) +
                '}';
    }
}
