//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

final class JacksonArrayProvider implements JacksonIteratorProvider {

    public static JacksonArrayProvider of(final Value elementOptions) {
        return new JacksonArrayProvider(Mixin.of(elementOptions).processor("<root>"));
    }

    private final ValueProcessor processor;

    JacksonArrayProvider(ValueProcessor processor) {
        this.processor = Objects.requireNonNull(processor);
    }

    @Override
    public JacksonIterator iterator(final JsonParser parser, final int bufferSize) throws IOException {
        return new JacksonArrayIterator(processor, parser, bufferSize);
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return processor.columnTypes();
    }
}
