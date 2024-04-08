//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.google.auto.service.AutoService;
import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.ValueOptions;

/**
 * The Jackson {@link JsonProcessorProvider}.
 *
 * @see JacksonProcessors
 */
@AutoService({JsonProcessorProvider.class})
public final class JacksonJsonProvider implements JsonProcessorProvider {

    @Override
    public JacksonProcessors provider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonConfiguration.defaultFactory());
    }

    @Override
    public JacksonProcessors namedProvider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonConfiguration.defaultFactory());
    }
}
