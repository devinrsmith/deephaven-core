//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.stream.Stream;

public interface JacksonIteratorProvider {

    static JacksonIteratorProvider array(final Value options) {
        return JacksonArrayProvider.of(options);
    }

    static JacksonIteratorProvider stream(final Value options) {
        return JacksonStreamProvider.of(options);
    }

    JacksonIterator iterator(final JsonParser parser, final int bufferSize) throws IOException;

    Stream<Type<?>> columnTypes();
}
