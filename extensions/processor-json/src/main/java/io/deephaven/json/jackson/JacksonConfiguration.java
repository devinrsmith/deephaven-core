/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.ObjectCodec;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

public final class JacksonConfiguration {

    private static final String OBJECT_MAPPER_CLASS = "com.fasterxml.jackson.databind.ObjectMapper";
    private static final JacksonConfiguration DEFAULT_INSTANCE;

    static {
        // We'll attach an ObjectMapper if it's on the classpath, this allows parsing of AnyOptions
        ObjectCodec objectCodec = null;
        try {
            final Class<?> clazz = Class.forName(OBJECT_MAPPER_CLASS);
            objectCodec = (ObjectCodec) clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            // ignore
        }
        // todo: are there any configurations we want to set by default?
        DEFAULT_INSTANCE = new JacksonConfiguration(new JsonFactory(objectCodec));
    }

    /**
     * The default configuration. If the optional jackson-databind package is on the classpath, the configuration will
     * support parsing of {@link io.deephaven.json.AnyOptions}.
     *
     * @return the default configuration instance
     */
    public static JacksonConfiguration defaultInstance() {
        return DEFAULT_INSTANCE;
    }

    /**
     * Creates a new configuration using the provided {@code factory}.
     *
     * @param factory the factory
     * @return the configuration
     */
    public static JacksonConfiguration of(JsonFactory factory) {
        return new JacksonConfiguration(factory);
    }

    private final JsonFactory factory;

    private JacksonConfiguration(JsonFactory factory) {
        this.factory = Objects.requireNonNull(factory);
    }

    JsonFactory factory() {
        return factory;
    }
}
