//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public interface JacksonIteratorProvider {

    static JacksonIteratorProvider array(final Value options) {
        return Mixin.of(options).arrayProvider();
    }

    static JacksonIteratorProvider stream(final Value options) {
        return Mixin.of(options).streamProvider();
    }

    Value options();

    JacksonIterator iterator(final JsonParser parser, final int bufferSize) throws IOException;

    List<Type<?>> outputTypes();

    int outputSize();

    List<String> names();

    List<String> names(Function<List<String>, String> f);
}
