/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.google.auto.service.AutoService;
import io.deephaven.json.JsonProvider;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

@SuppressWarnings("unused")
@AutoService(JsonProvider.class)
public final class JacksonJsonProvider implements JsonProvider {

    @Override
    public ObjectProcessor.Provider provider(ValueOptions options) {
        return JacksonProvider.of(options);
    }

    @Override
    public NamedObjectProcessor.Provider namedProvider(ValueOptions options) {
        return JacksonProvider.of(options);
    }
}
