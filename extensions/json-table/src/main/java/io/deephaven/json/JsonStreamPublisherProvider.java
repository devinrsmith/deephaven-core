/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.engine.table.Table;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface JsonStreamPublisherProvider {

    static JsonStreamPublisherProvider serviceLoader() {
        final Iterator<JsonStreamPublisherProvider> it = ServiceLoader.load(JsonStreamPublisherProvider.class).iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final JsonStreamPublisherProvider provider = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return provider;
    }

    JsonStreamPublisher of(JsonStreamPublisherOptions options);

    Table of(JsonTableOptions options);
}
