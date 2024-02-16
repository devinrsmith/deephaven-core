/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.bson.jackson;

import com.fasterxml.jackson.core.ObjectCodec;
import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.JacksonProvider;

import java.lang.reflect.InvocationTargetException;

/**
 * A specific BSON processor implementation using bson4jackson. This provides more control over the default
 * {@link ValueOptions#processor(Class)} and {@link ValueOptions#named(Class)}.
 */
public final class JacksonBsonProvider {
    private static final BsonFactory DEFAULT_FACTORY;

    static {
        // We'll attach an ObjectMapper if it's on the classpath, this allows parsing of AnyOptions
        ObjectCodec objectCodec = null;
        try {
            final Class<?> clazz = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
            objectCodec = (ObjectCodec) clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            // ignore
        }
        // todo: are there any configurations we want to set by default?
        DEFAULT_FACTORY = new BsonFactory(objectCodec);
    }

    /**
     * Creates a jackson bson provider using the default bson factory. If the optional jackson-databind package is on
     * the classpath, the provider will be capable of parsing {@link io.deephaven.json.AnyOptions}.
     *
     * @param options the object options
     * @return the jackson bson provider
     */
    static JacksonProvider of(ValueOptions options) {
        return of(options, DEFAULT_FACTORY);
    }

    /**
     * Creates a jackson bson provider using the provided bson {@code factory}.
     *
     * @param options the object options
     * @param factory the jackson bson factory
     * @return the jackson bson provider
     */
    static JacksonProvider of(ValueOptions options, BsonFactory factory) {
        return JacksonProvider.of(options, factory);
    }
}
