/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.ObjectCodec;
import io.deephaven.json.JsonConfiguration;
import io.deephaven.json.ValueOptions;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

public final class JacksonConfiguration implements JsonConfiguration {

    private static final JacksonConfiguration DEFAULT_INSTANCE;

    static {
        // We'll attach an ObjectMapper if it's on the classpath
        ObjectCodec objectCodec = null;
        try {
            final Class<?> clazz = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
            objectCodec = (ObjectCodec) clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            // ignore
        }
        DEFAULT_INSTANCE = new JacksonConfiguration(new JsonFactory(objectCodec));
    }

    public static JacksonConfiguration defaultInstance() {
        return DEFAULT_INSTANCE;
    }

    private final JsonFactory factory;

    public JacksonConfiguration(JsonFactory factory) {
        this.factory = Objects.requireNonNull(factory);
    }

    @Override
    public JacksonProcessor namedProvider(ValueOptions options) {
        return JacksonProcessor.of(options, factory);
    }

    @Override
    public JacksonProcessor processorProvider(ValueOptions options) {
        return JacksonProcessor.of(options, factory);
    }
}
