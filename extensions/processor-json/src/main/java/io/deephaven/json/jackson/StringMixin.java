/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.StringOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class StringMixin extends Mixin {
    private final StringOptions options;

    public StringMixin(StringOptions options, JacksonConfiguration factory) {
        super(factory);
        this.options = Objects.requireNonNull(options);
    }

    @Override
    public int outputCount() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.stringType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
    }

    private String parseString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseNumberInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseNumberFloat(JsonParser parser) throws IOException {
        if (!options.allowNumberFloat()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseBoolean(JsonParser parser) throws IOException {
        if (!options.allowBoolean()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return parser.getText();
    }

    private String parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, String.class);
        }
        return options.onNull().orElse(null);
    }

    private String parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, String.class);
        }
        return options.onMissing().orElse(null);
    }

    private class Impl implements ObjectValueProcessor.ToObject<String> {
        @Override
        public String parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NUMBER_INT:
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    return parseNumberFloat(parser);
                case VALUE_TRUE:
                case VALUE_FALSE:
                    return parseBoolean(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, String.class);
        }

        @Override
        public String parseMissing(JsonParser parser) throws IOException {
            return StringMixin.this.parseMissing(parser);
        }
    }
}
