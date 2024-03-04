/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.BigDecimalOptions;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;

final class BigDecimalMixin extends Mixin<BigDecimalOptions> {

    public BigDecimalMixin(BigDecimalOptions options, JsonFactory factory) {
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
        return Stream.of(Type.ofCustom(BigDecimal.class));
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return ObjectValueProcessor.of(out.get(0).asWritableObjectChunk(), new Impl());
    }

    @Override
    ArrayProcessor arrayProcessor(ArrayOptions options, List<WritableChunk<?>> out) {
        // array of arrays
        throw new UnsupportedOperationException("todo");
    }

    private BigDecimal parseNumber(JsonParser parser) throws IOException {
        if (!options.allowNumberInt() && !options.allowDecimal()) {
            throw Helpers.mismatch(parser, BigDecimal.class);
        }
        return Helpers.parseDecimalAsBigDecimal(parser);
    }

    private BigDecimal parseString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Helpers.mismatch(parser, BigDecimal.class);
        }
        return Helpers.parseStringAsBigDecimal(parser);
    }

    private BigDecimal parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, BigDecimal.class);
        }
        return options.onNull().orElse(null);
    }

    private BigDecimal parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, BigDecimal.class);
        }
        return options.onMissing().orElse(null);
    }

    private class Impl implements ToObject<BigDecimal> {
        @Override
        public BigDecimal parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    return parseNumber(parser);
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, BigDecimal.class);
        }

        @Override
        public BigDecimal parseMissing(JsonParser parser) throws IOException {
            return BigDecimalMixin.this.parseMissing(parser);
        }
    }
}
