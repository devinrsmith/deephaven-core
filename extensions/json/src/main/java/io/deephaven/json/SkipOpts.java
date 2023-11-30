/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Stream;

// no builder
final class SkipOpts extends ValueOptions {
    @Override
    public boolean allowNull() {
        return true;
    }

    @Override
    public boolean allowMissing() {
        return true;
    }

    @Override
    ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return ValueProcessor.skip();
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.empty();
    }
}
