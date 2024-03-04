/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.IntOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class IntMixin extends Mixin<IntOptions> {
    public IntMixin(IntOptions options, JsonFactory factory) {
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
        return Stream.of(Type.intType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new IntValueProcessor(out.get(0).asWritableIntChunk(), new Impl());
    }

    IntValueProcessor.ToInt intImpl() {
        return new Impl();
    }

    private int parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return Helpers.parseIntAsInt(parser);
    }

    private int parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowDecimal()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return Helpers.parseDecimalAsTruncatedInt(parser);
    }

    private int parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, int.class);
        }
        return options.allowDecimal()
                ? Helpers.parseDecimalStringAsTruncatedInt(parser)
                : Helpers.parseStringAsInt(parser);
    }

    private int parseFromNull(JsonParser parser) throws IOException {
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
                    return parseFromInt(parser);
                case VALUE_NUMBER_FLOAT:
                    return parseFromDecimal(parser);
                case VALUE_STRING:
                    return parseFromString(parser);
                case VALUE_NULL:
                    return parseFromNull(parser);
            }
            throw Helpers.mismatch(parser, int.class);
        }

        @Override
        public int parseMissing(JsonParser parser) throws IOException {
            return IntMixin.this.parseMissing(parser);
        }
    }
}
