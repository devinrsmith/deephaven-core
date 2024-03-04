/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.CharOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class CharMixin extends Mixin<CharOptions> {
    public CharMixin(CharOptions options, JsonFactory factory) {
        super(factory, options);
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
        return Stream.of(Type.charType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new CharValueProcessor(out.get(0).asWritableCharChunk(), charImpl());
    }

    @Override
    ArrayProcessor arrayProcessor(ArrayOptions options, List<WritableChunk<?>> out) {
        // array of arrays
        throw new UnsupportedOperationException("todo");
    }

    CharValueProcessor.ToChar charImpl() {
        return new Impl();
    }

    private char parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, char.class);
        }
        return Helpers.parseStringAsChar(parser);
    }

    private char parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, char.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_CHAR);
    }

    private char parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, char.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_CHAR);
    }

    private class Impl implements CharValueProcessor.ToChar {
        @Override
        public char parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_STRING:
                    return parseFromString(parser);
                case VALUE_NULL:
                    return parseFromNull(parser);
            }
            throw Helpers.mismatch(parser, int.class);
        }

        @Override
        public char parseMissing(JsonParser parser) throws IOException {
            return CharMixin.this.parseMissing(parser);
        }
    }
}
