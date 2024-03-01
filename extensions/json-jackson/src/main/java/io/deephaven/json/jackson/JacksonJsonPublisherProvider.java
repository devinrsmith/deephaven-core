/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.JsonPublishingProvider;
import io.deephaven.json.JsonStreamPublisher;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

/**
 * The Jackson {@link JsonPublishingProvider} and {@link JsonPublishingProvider}.
 *
 * @see JacksonProvider
 * @see JacksonStreamPublisher
 * @see JacksonTable
 */
@AutoService({JsonProcessorProvider.class, JsonPublishingProvider.class})
public final class JacksonJsonPublisherProvider implements JsonProcessorProvider, JsonPublishingProvider {

    @Override
    public ObjectProcessor.Provider provider(ValueOptions options) {
        return JacksonProvider.of(options);
    }

    @Override
    public NamedObjectProcessor.Provider namedProvider(ValueOptions options) {
        return JacksonProvider.of(options);
    }

    @Override
    public JsonStreamPublisher of(JsonStreamPublisherOptions options) {
        return JacksonStreamPublisher.of(options);
    }

    @Override
    public Table of(JsonTableOptions options) {
        return JacksonTable.of(options);
    }
}
