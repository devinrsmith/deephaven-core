//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.function.Function;

public interface Yep {

    // io.deephaven.processor.ObjectProcessor.Provider.outputTypes
    List<Type<?>> outputTypes();

    // io.deephaven.processor.ObjectProcessor.outputSize
    int outputSize();

    // io.deephaven.processor.NamedObjectProcessor.Provider.names
    List<String> names();

    // io.deephaven.json.jackson.JacksonProvider.names
    List<String> names(Function<List<String>, String> f);
}
