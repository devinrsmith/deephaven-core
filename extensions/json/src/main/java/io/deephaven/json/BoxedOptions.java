//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;

import java.util.Optional;

public abstract class BoxedOptions<T> extends ValueOptions {

    public abstract Optional<T> onNull();

    public abstract Optional<T> onMissing();

    public interface Builder<T, V extends BoxedOptions<T>, B extends Builder<T, V, B>>
            extends ValueOptions.Builder<V, B> {
        B onNull(T onNull);

        B onMissing(T onMissing);
    }

    @Check
    final void checkOnNull() {
        if (!allowedTypes().contains(JsonValueTypes.NULL) && onNull().isPresent()) {
            throw new IllegalArgumentException("onNull set, but NULL is not allowed");
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException("onMissing set, but allowMissing is false");
        }
    }
}
