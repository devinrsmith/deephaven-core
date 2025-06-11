//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;

import java.util.Objects;
import java.util.stream.Stream;

final class JacksonStreamProvider implements JacksonIteratorProvider {

    public static JacksonStreamProvider of(final Value options) {
        return new JacksonStreamProvider(Mixin.of(options).processor("<root>"));
    }

    private final ValueProcessor processor;

    JacksonStreamProvider(ValueProcessor processor) {
        this.processor = Objects.requireNonNull(processor);
    }

    @Override
    public JacksonIterator iterator(final JsonParser parser, final int bufferSize) {
        return new JacksonStreamIterator(processor, parser, bufferSize);
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return processor.columnTypes();
    }
}
