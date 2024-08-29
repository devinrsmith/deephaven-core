//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.primitive.function;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Functional interface to apply an operation to a single {@code boolean}.
 */
@FunctionalInterface
public interface BooleanConsumer {

    /**
     * Apply this operation to {@code value}.
     *
     * @param value The {@code boolean} to operate one
     */
    void accept(boolean value);

    /**
     * Return a composed CharConsumer that applies {@code this} operation followed by {@code after}.
     *
     * @param after The CharConsumer to apply after applying {@code this}
     * @return A composed CharConsumer that applies {@code this} followed by {@code after}
     */
    default BooleanConsumer andThen(@NotNull final BooleanConsumer after) {
        Objects.requireNonNull(after);
        return (final boolean value) -> {
            accept(value);
            after.accept(value);
        };
    }
}
