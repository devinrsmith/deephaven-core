/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class NavContext {

    interface JsonProcess {

        void process(JsonParser parser) throws IOException;
    }

    static void processObjectField(JsonParser parser, String fieldName, JsonProcess inner) throws IOException {
        if (!parser.hasToken(JsonToken.START_OBJECT)) {
            throw new IOException("Expected START_OBJECT");
        }
        parser.nextToken();
        // Only process the first field with this name
        boolean processed = false;
        while (parser.hasToken(JsonToken.FIELD_NAME)) {
            if (!processed && fieldName.equals(parser.currentName())) {
                parser.nextToken();
                inner.process(parser);
                processed = true;
                continue;
            }
            // field value, skip
            skipField(parser);
        }
        if (!parser.hasToken(JsonToken.END_OBJECT)) {
            throw new IOException("Expected END_OBJECT");
        }
        parser.nextToken();
    }

    static void processObjectField(JsonParser parser, List<String> fieldPath, JsonProcess consumer)
            throws IOException {
        if (fieldPath.isEmpty()) {
            consumer.process(parser);
            return;
        }
        final String fieldName = fieldPath.get(0);
        if (fieldPath.size() == 1) {
            processObjectField(parser, fieldName, consumer);
            return;
        }
        final List<String> remaining = fieldPath.subList(1, fieldPath.size());
        processObjectField(parser, fieldName, new ObjectFieldsList(remaining, consumer));
    }

    static JsonProcess singleFieldProcess(List<String> fieldPath, JsonProcess consumer) {
        return fieldPath.isEmpty() ? consumer : new ObjectFieldsList(fieldPath, consumer);
    }

    private static void skipField(JsonParser parser) throws IOException {
        // field value, skip
        parser.nextToken();
        parser.skipChildren();
        // setup next field name, or end object
        parser.nextToken();
    }

    private static final class ObjectFieldsList implements JsonProcess {
        private final List<String> fieldPath;
        private final JsonProcess consumer;

        public ObjectFieldsList(List<String> fieldPath, JsonProcess consumer) {
            this.fieldPath = Objects.requireNonNull(fieldPath);
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            processObjectField(parser, fieldPath, consumer);
        }
    }
}
