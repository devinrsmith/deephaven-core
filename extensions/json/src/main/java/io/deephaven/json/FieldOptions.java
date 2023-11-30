/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Stream;

public abstract class FieldOptions {

    public static FieldOptions skip() {
        return new SkipOpts();
    }

    public abstract boolean allowNull();

    public abstract boolean allowMissing();

    abstract ValueProcessor processor(String context, List<WritableChunk<?>> out);

    abstract Stream<Type<?>> outputTypes();

    final int numColumns() {
        return (int) outputTypes().count();
    }

    interface Builder<V extends FieldOptions, B extends Builder<V, B>> {
        B allowNull(boolean allowNull);

        B allowMissing(boolean allowMissing);

        V build();
    }
}
