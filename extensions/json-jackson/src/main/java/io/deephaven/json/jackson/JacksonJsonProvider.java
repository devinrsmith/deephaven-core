//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.auto.service.AutoService;
import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.ValueOptions;
import io.deephaven.qst.type.Type;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

/**
 * The Jackson {@link JsonProcessorProvider}.
 *
 * @see JacksonProcessors
 */
@AutoService({JsonProcessorProvider.class})
public final class JacksonJsonProvider implements JsonProcessorProvider {

    /**
     * The jackson supported types. Equivalent to {@link JacksonProcessors#getSupportedTypes()}}.
     *
     * @return the supported types
     */
    @Override
    public List<Type<?>> supportedTypes() {
        return JacksonProcessors.getSupportedTypes();
    }

    /**
     * Equivalent to {@code JacksonProcessors.of(options, JacksonConfiguration.defaultFactory())}.
     *
     * @param options the options
     * @return the object processor provider
     * @see JacksonProcessors#of(ValueOptions, JsonFactory)
     * @see JacksonConfiguration#defaultFactory()
     */
    @Override
    public JacksonProcessors provider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonConfiguration.defaultFactory());
    }

    /**
     * Equivalent to {@code JacksonProcessors.of(options, JacksonConfiguration.defaultFactory())}.
     *
     * @param options the options
     * @return the named object processor provider
     * @see JacksonProcessors#of(ValueOptions, JsonFactory)
     * @see JacksonConfiguration#defaultFactory()
     */
    @Override
    public JacksonProcessors namedProvider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonConfiguration.defaultFactory());
    }
}
