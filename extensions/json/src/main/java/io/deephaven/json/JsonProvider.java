/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface JsonProvider {

    static JsonProvider serviceLoader() {
        final Iterator<JsonProvider> it = ServiceLoader.load(JsonProvider.class).iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final JsonProvider provider = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return provider;
    }

    ObjectProcessor.Provider provider(ValueOptions options);

    NamedObjectProcessor.Provider namedProvider(ValueOptions options);
}
