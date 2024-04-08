//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.JacksonProcessors;

// Not hooking up auto-service, is not the default
public final class JacksonBsonProvider implements JsonProcessorProvider {

    @Override
    public JacksonProcessors provider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonBsonConfiguration.defaultFactory());
    }

    @Override
    public JacksonProcessors namedProvider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonBsonConfiguration.defaultFactory());
    }
}
