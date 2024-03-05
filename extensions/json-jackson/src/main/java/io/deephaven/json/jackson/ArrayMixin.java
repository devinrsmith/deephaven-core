/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ArrayOptions;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Stream;

final class ArrayMixin extends Mixin<ArrayOptions> {

    public ArrayMixin(ArrayOptions options, JsonFactory factory) {
        super(factory, options);
    }

    final boolean asArray() {
        // todo, expose to user, provide option to use ChunkProvider (ie, multiple rows)
        return true;
    }

    Mixin<?> element() {
        return mixin(options.element());
    }

    @Override
    public int outputCount() {
        return element().outputCount();
    }

    @Override
    public Stream<List<String>> paths() {
        return element().paths();
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return element().outputTypes().map(Type::arrayType);
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ValueProcessorArrayImpl(element().arrayProcessor(options.allowMissing(), options.allowNull(), out));
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        // array of arrays
        throw new UnsupportedOperationException("todo");
    }
}
