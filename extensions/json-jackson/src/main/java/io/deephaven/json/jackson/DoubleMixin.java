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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class DoubleMixin extends Mixin<DoubleOptions> {

    public DoubleMixin(DoubleOptions options, JsonFactory factory) {
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
        return Stream.of(Type.doubleType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new DoubleValueProcessor(out.get(0).asWritableDoubleChunk(), doubleImpl());
    }

    Impl doubleImpl() {
        return new Impl();
    }

    private double onNullOrDefault() {
        return options.onNull().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double onMissingOrDefault() {
        return options.onMissing().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double parseFromNumber(JsonParser parser) throws IOException {
        if (!options.allowDecimal() && !options.allowNumberInt()) {
            throw Helpers.mismatch(parser, double.class);
        }
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return Helpers.parseNumberAsDouble(parser);
    }

    private double parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, double.class);
        }
        return Helpers.parseStringAsDouble(parser);
    }

    private double parseFromNull(JsonParser parser) throws IOException {
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
                    return parseFromNumber(parser);
                case VALUE_STRING:
                    return parseFromString(parser);
                case VALUE_NULL:
                    return parseFromNull(parser);
            }
            throw Helpers.mismatch(parser, double.class);
        }

        @Override
        public double parseMissing(JsonParser parser) throws IOException {
            return DoubleMixin.this.parseMissing(parser);
        }
    }
}
