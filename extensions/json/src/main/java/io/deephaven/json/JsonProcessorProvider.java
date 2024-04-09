//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface JsonProcessorProvider {
    /**
     * Returns the single service-loader {@link JsonProcessorProvider}. If there are none or more than one, throws an
     * {@link IllegalStateException}.
     *
     * @return the service-loader json processor provider
     */
    static JsonProcessorProvider serviceLoader() {
        final Iterator<JsonProcessorProvider> it = ServiceLoader.load(JsonProcessorProvider.class).iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final JsonProcessorProvider provider = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return provider;
    }

    /**
     * Create an object processor provider for the given JSON options.
     *
     * @param options the options
     * @return the object processor provider
     */
    ObjectProcessor.Provider provider(ValueOptions options);

    /**
     * Create a named object processor provider for the given JSON options.
     *
     * @param options the options
     * @return the named object processor provider
     */
    NamedObjectProcessor.Provider namedProvider(ValueOptions options);
}
