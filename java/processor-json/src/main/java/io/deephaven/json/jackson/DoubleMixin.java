/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.DoubleOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class DoubleMixin extends Mixin {
    private final DoubleOptions options;

    public DoubleMixin(DoubleOptions options, JsonFactory factory) {
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
        return Stream.of(Type.doubleType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new DoubleValueProcessor(out.get(0).asWritableDoubleChunk(), new Impl());
    }

    private double onNullOrDefault() {
        return options.onNull().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double onMissingOrDefault() {
        return options.onMissing().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double parseNumberIntOrFloat(JsonParser parser) throws IOException {
        if (!options.allowNumber()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return parser.getDoubleValue();
    }

    private double parseString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return Helpers.parseStringAsDouble(parser);
    }

    private double parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return onNullOrDefault();
    }

    private double parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, double.class);
        }
        return onMissingOrDefault();
    }

    class Impl implements DoubleValueProcessor.ToDouble {
        @Override
        public double parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return parseNumberIntOrFloat(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, double.class);
        }

        @Override
        public double parseMissing(JsonParser parser) throws IOException {
            return DoubleMixin.this.parseMissing(parser);
        }
    }
}
