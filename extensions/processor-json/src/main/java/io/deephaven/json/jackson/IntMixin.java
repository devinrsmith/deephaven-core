/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.IntOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class IntMixin extends Mixin {
    private final IntOptions options;

    public IntMixin(IntOptions options, JacksonConfiguration factory) {
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
        return Stream.of(Type.intType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new IntValueProcessor(out.get(0).asWritableIntChunk(), new Impl());
    }

    private int parseNumberInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return parser.getIntValue();
    }

    private int parseNumberFloat(JsonParser parser) throws IOException {
        if (!options.allowNumberFloat()) {
            throw Helpers.mismatch(parser, int.class);
        }
        // May lose info
        return parser.getIntValue();
    }

    private int parseString(JsonParser parser) throws IOException {
        switch (options.allowString()) {
            case NONE:
                throw Helpers.mismatch(parser, int.class);
            case INT:
                return Helpers.parseStringAsInt(parser);
            case FLOAT:
                // Need to parse as double to have 32-bit int range
                return (int) Helpers.parseStringAsDouble(parser);
        }
        throw new IllegalStateException();
    }

    private int parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_INT);
    }

    private int parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, int.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_INT);
    }

    private class Impl implements IntValueProcessor.ToInt {
        @Override
        public int parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    return parseNumberFloat(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, int.class);
        }

        @Override
        public int parseMissing(JsonParser parser) throws IOException {
            return IntMixin.this.parseMissing(parser);
        }
    }
}
