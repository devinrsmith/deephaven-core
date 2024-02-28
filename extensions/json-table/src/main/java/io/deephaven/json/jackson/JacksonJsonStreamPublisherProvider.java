/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import io.deephaven.engine.table.Table;
import io.deephaven.json.JsonStreamPublisher;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.JsonStreamPublisherProvider;
import io.deephaven.json.JsonTableOptions;

// todo autoservice
public final class JacksonJsonStreamPublisherProvider implements JsonStreamPublisherProvider {

    @Override
    public JsonStreamPublisher of(JsonStreamPublisherOptions options) {
        return JacksonStreamPublisher.of(options);
    }

    @Override
    public Table of(JsonTableOptions options) {
        return JacksonTable.of(options);
    }
}
