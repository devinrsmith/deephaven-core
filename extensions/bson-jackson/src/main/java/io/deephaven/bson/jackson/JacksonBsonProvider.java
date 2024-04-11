//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.JacksonProcessors;
import io.deephaven.qst.type.Type;

import java.util.Set;

// Not hooking up auto-service, is not the default
public final class JacksonBsonProvider implements JsonProcessorProvider {

    /**
     * Creates a jackson BSON provider using the provided BSON {@code factory}. Equivalent to
     * {@code JacksonProcessors.of(options, factory)}.
     *
     * @param options the options
     * @param factory the BSON factory
     * @return the jackson BSON provider
     * @see JacksonProcessors#of(ValueOptions, JsonFactory)
     */
    public static JacksonProcessors of(ValueOptions options, BsonFactory factory) {
        return JacksonProcessors.of(options, factory);
    }

    /**
     * The jackson BSON supported types. Equivalent to {@link JacksonProcessors#getSupportedTypes()}.
     *
     * @return the supported types
     */
    @Override
    public Set<Type<?>> supportedTypes() {
        return JacksonProcessors.getSupportedTypes();
    }

    /**
     * Creates a jackson BSON object processor provider with a default BSON factory.
     *
     * @param options the options
     * @return the object processor provider
     * @see #of(ValueOptions, BsonFactory)
     */
    @Override
    public JacksonProcessors provider(ValueOptions options) {
        return of(options, JacksonBsonConfiguration.defaultFactory());
    }

    /**
     * Creates a jackson BSON named object processor provider with a default BSON factory.
     *
     * @param options the options
     * @return the object processor provider
     * @see #of(ValueOptions, BsonFactory)
     */
    @Override
    public JacksonProcessors namedProvider(ValueOptions options) {
        return of(options, JacksonBsonConfiguration.defaultFactory());
    }
}
