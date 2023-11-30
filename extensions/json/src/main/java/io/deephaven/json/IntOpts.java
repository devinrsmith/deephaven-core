/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Default;

import java.util.List;
import java.util.stream.Stream;

public class IntOpts extends FieldOptions {
    public static Builder builder() {
        return null;
    }

    @Override
    @Default
    public boolean allowNull() {
        return true;
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new IntChunkValueProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableIntChunk());
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.of(Type.intType());
    }

    public interface Builder extends FieldOptions.Builder<IntOpts, Builder> {

    }
}
