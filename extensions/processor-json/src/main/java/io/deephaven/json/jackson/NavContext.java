/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.jackson.PathToSingleValue.ObjectField;
import io.deephaven.json.jackson.PathToSingleValue.Path;
import io.deephaven.json.jackson.PathToSingleValue.ArrayIndex;

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

    static void processTupleIndex(JsonParser parser, int index, JsonProcess inner) throws IOException {
        if (!parser.hasToken(JsonToken.START_ARRAY)) {
            throw new IOException("Expected START_ARRAY");
        }
        parser.nextToken();
        for (int i = 0; i < index; ++i) {
            skipElement(parser);
        }
        inner.process(parser);
        while (!parser.hasToken(JsonToken.END_ARRAY)) {
            skipElement(parser);
        }
        parser.nextToken();
    }

    static void processPath(JsonParser parser, Path path, JsonProcess inner) throws IOException {
        if (path instanceof ObjectField) {
            processObjectField(parser, ((ObjectField) path).field(), inner);
            return;
        }
        if (path instanceof ArrayIndex) {
            processTupleIndex(parser, ((ArrayIndex) path).index(), inner);
            return;
        }
        throw new IllegalStateException();
    }

    static void processPath(JsonParser parser, List<Path> paths, JsonProcess consumer)
            throws IOException {
        if (paths.isEmpty()) {
            consumer.process(parser);
            return;
        }
        final Path path = paths.get(0);
        if (paths.size() == 1) {
            processPath(parser, path, consumer);
            return;
        }
        final List<Path> remaining = paths.subList(1, paths.size());
        processPath(parser, path, new ProcessPath(remaining, consumer));
    }

    // static JsonProcess singleFieldProcess(List<String> fieldPath, JsonProcess consumer) {
    // return fieldPath.isEmpty() ? consumer : new ProcessPath(fieldPath, consumer);
    // }

    private static void skipField(JsonParser parser) throws IOException {
        // field value, skip
        skipElement(parser);
        // setup next field name, or end object
        parser.nextToken();
    }

    private static void skipElement(JsonParser parser) throws IOException {
        parser.nextToken();
        parser.skipChildren();
    }

    private static final class ProcessPath implements JsonProcess {
        private final List<Path> paths;
        private final JsonProcess consumer;

        public ProcessPath(List<Path> paths, JsonProcess consumer) {
            this.paths = Objects.requireNonNull(paths);
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            processPath(parser, paths, consumer);
        }
    }
}
