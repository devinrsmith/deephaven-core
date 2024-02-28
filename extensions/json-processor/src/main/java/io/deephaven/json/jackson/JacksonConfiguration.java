/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.ObjectCodec;

import java.lang.reflect.InvocationTargetException;

final class JacksonConfiguration {

    private static final JsonFactory DEFAULT_FACTORY;

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
        DEFAULT_FACTORY = new JsonFactory(objectCodec);
    }

    public static JsonFactory defaultFactory() {
        return DEFAULT_FACTORY;
    }
}
