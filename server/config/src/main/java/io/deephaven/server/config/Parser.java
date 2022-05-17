package io.deephaven.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

class Parser {
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.findAndRegisterModules();
    }

    static <T> T parseJson(File file, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(file, clazz);
    }

    static <T> T parseJson(String value, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(value, clazz);
    }
}
