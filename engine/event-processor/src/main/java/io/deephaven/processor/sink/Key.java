//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.qst.type.Type;

import java.util.Objects;

public final class Key<T> {

    public static <T> Key<T> of(String debugName, Type<T> type) {
        return new Key<>(debugName, type);
    }

    private final Type<T> type;
    private final String name;

    private Key(String debugName, Type<T> type) {
        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(debugName);
    }

    public Type<T> type() {
        return type;
    }

    @Override
    public String toString() {
        return name;
    }
}
