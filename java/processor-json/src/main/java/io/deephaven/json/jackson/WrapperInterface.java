/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ValueOptions;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Stream;

interface WrapperInterface {

    static WrapperInterface of(ValueOptions options) {
        return null;
    }

    ValueProcessor processor(String context, List<WritableChunk<?>> out);

    int outputCount();

    // todo: is Map<List<String>, Type<?>> easier?
    // or, Stream<(List<String>, Type<?>)>?
    Stream<List<String>> paths();

    Stream<Type<?>> outputTypes();

}
